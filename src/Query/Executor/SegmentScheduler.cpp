#include "SegmentScheduler.h"
#include <optional>
#include <set>
#include <butil/endpoint.h>
#include <Common/ProfileEvents.h>
#include <Query/Executor/ProfileLogHub.h>
#include <Query/Executor/MPPScheduler.h>
#include <Query/Exchange/bRPC/WriteBufferFromBrpc.h>
#include <Query/Exchange/RpcChannelPool.h>
#include <Query/Exchange/RpcClient.h>


namespace ProfileEvents
{
extern const Event ScheduleTimeMilliseconds;
}
namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING;
    extern const int QUERY_CPU_TIMEOUT_EXCEEDED;
}



PlanSegmentsStatusPtr
SegmentScheduler::insertPlanSegments(const String & query_id, PlanSegmentTree * plan_segments_ptr, ContextPtr query_context)
{
    std::shared_ptr<DAGGraph> dag_ptr = std::make_shared<DAGGraph>();
    buildDAGGraph(plan_segments_ptr, dag_ptr);
    dag_ptr->setContext(query_context);
    {
        std::unique_lock<std::mutex> lock(mutex);
        if (query_map.find(query_id) != query_map.end())
        {
            // cancel running query
            if (query_context->getSettingsRef().replace_running_query)
            {
                //TODO support replace running query
                throw Exception(ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING,
                    "Query with id = {} is already running and replace_running_query is not supported now.", query_id);
            }
            else
                throw Exception(ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING, "Query with id = {} is already running.", query_id);
        }
        query_map.emplace(std::make_pair(query_id, dag_ptr));
    }

    {
        std::unique_lock<std::mutex> lock(segment_status_mutex);
        segment_status_map[query_id];
        query_status_map.emplace(query_id, std::make_shared<RuntimeSegmentStatus>());
    }

    if (query_context->getOptimizerContext()->getSettingsRef().report_segment_profiles)
    {
        std::unique_lock<std::mutex> lock(segment_profile_mutex);
        segment_profile_map[query_id];
    }

    auto * final_segment = plan_segments_ptr->getRoot()->getPlanSegment();
    auto local_address = getLocalAddress(query_context);
    final_segment->setCoordinatorAddress(local_address);
    //fast path for single node query
    if (plan_segments_ptr->getNodes().size() == 1)
    {
        dag_ptr->plan_segment_status_ptr->final_execution_info = PlanSegmentExecutionInfo{.parallel_id = 0};
        return dag_ptr->plan_segment_status_ptr;
    }
    prepareQueryCommonBuf(dag_ptr->query_common_buf, *final_segment, query_context);
    WriteBufferFromBrpc settings_write_buf;
    query_context->getSettingsRef().write(settings_write_buf, SettingsWriteFormat::STRINGS_WITH_FLAGS);
    query_context->getOptimizerContext()->getSettingsRef().write(settings_write_buf, SettingsWriteFormat::STRINGS_WITH_FLAGS);
    settings_write_buf.finalize();
    dag_ptr->query_settings_buf.append(settings_write_buf.getFinishedBuf().movable());
    
    if (!dag_ptr->plan_segment_status_ptr->is_final_stage_start)
    {
        dag_ptr->plan_segment_status_ptr->final_execution_info = scheduleV2(query_id, query_context, dag_ptr);
    }

#if defined(TASK_ASSIGN_DEBUG)
    String res;
    res += "dump statics:" + std::to_string(dag_ptr->exchange_data_assign_node_mappings.size()) + "\n";
    for (auto it = dag_ptr->exchange_data_assign_node_mappings.begin(); it != dag_ptr->exchange_data_assign_node_mappings.end(); it++)
    {
        res += "segment id: " + std::to_string(it->first);
        for (size_t j = 0; j < it->second.size(); j++)
        {
            res += "\n  index:" + std::to_string(it->second[j].first) + " address:" + it->second[j].second.getHostName() + "_"
                + std::to_string(it->second[j].second.getPort());
        }
        res += "\n";
    }
    LOG_DEBUG(log, "{}", res);

#endif
    return dag_ptr->plan_segment_status_ptr;
}

static void OnCancelQueryCallback(
    Protos::CancelQueryResponse * response, brpc::Controller * cntl, std::shared_ptr<RpcClient> rpc_client, String query_id)
{
    static auto log = getLogger("SegmentScheduler");

    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    std::unique_ptr<Protos::CancelQueryResponse> response_guard(response);

    rpc_client->checkAliveWithController(*cntl);
    if (cntl->Failed())
    {
        LOG_TRACE(
            log,
            "Send cancel query with id {} to {} failed, error: {}, msg: {}",
            query_id,
            butil::endpoint2str(cntl->remote_side()).c_str(),
            cntl->ErrorText(),
            response->message());
    }
    else
    {
        LOG_TRACE(log, "Send cancel query with id {} to {} success", query_id, butil::endpoint2str(cntl->remote_side()).c_str());
    }
}

CancellationCode SegmentScheduler::cancelPlanSegmentsFromCoordinator(
    const String & query_id, const Int32 & code, const String & exception, ContextPtr query_context)
{
    const String & coordinator_host = getHostIPFromEnv();
    return cancelPlanSegments(query_id, code, exception, coordinator_host, query_context);
}

CancellationCode SegmentScheduler::cancelPlanSegments(
    const String & query_id,
    const Int32 & code,
    const String & exception,
    const String & origin_host_name,
    ContextPtr query_context,
    std::shared_ptr<DAGGraph> dag_graph_ptr)
{
    std::shared_ptr<DAGGraph> dag_ptr;

    if (dag_graph_ptr == nullptr) // try to get the dag_graph_ptr
    {
        std::unique_lock<std::mutex> lock(mutex);
        auto query_map_ite = query_map.find(query_id);
        if (query_map_ite == query_map.end())
            return CancellationCode::NotFound;
        dag_ptr = query_map_ite->second;
    }
    else
    {
        dag_ptr = dag_graph_ptr;
    }

    {
        {
            std::unique_lock<std::mutex> lock(dag_ptr->status_mutex);
            LOG_ERROR(log, "query({}) receive error from host:{} with exception:{} and plan_send_addresses size:{}",
                    query_id, origin_host_name, exception, dag_ptr->plan_send_addresses.size());

            if (dag_ptr->plan_segment_status_ptr->is_cancel.load(std::memory_order_relaxed))
                return CancellationCode::CancelSent;
            dag_ptr->plan_segment_status_ptr->is_cancel.store(true, std::memory_order_relaxed);
            dag_ptr->plan_segment_status_ptr->error_code = code;
            dag_ptr->plan_segment_status_ptr->exception
                = "query(" + query_id + ") receive exception from host-" + origin_host_name + " with exception:" + exception;
        }

        /// send cancel query rpc request to all executor except exception original executor
        cancelWorkerPlanSegments(query_id, dag_ptr, query_context);
    }
    return CancellationCode::CancelSent;
}

void SegmentScheduler::cancelWorkerPlanSegments(const String & query_id, const DAGGraphPtr dag_ptr, ContextPtr query_context)
{
    String coordinator_addr = query_context->getOptimizerContext()->getHostWithPorts().getExchangeAddress();
    std::vector<brpc::CallId> call_ids;
    std::set<AddressInfo> plan_send_addresses;
    {
        std::unique_lock<std::mutex> lock(dag_ptr->status_mutex);
        plan_send_addresses = dag_ptr->plan_send_addresses;
    }
    Protos::CancelQueryRequest request;
    request.set_query_id(query_id);
    request.set_coordinator_address(coordinator_addr);

    for (const auto & addr : plan_send_addresses)
    {
        auto address = extractExchangeHostPort(addr);
        std::shared_ptr<RpcClient> rpc_client = RpcChannelPool::getInstance().getClient(address, BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY);
        Protos::PlanSegmentService_Stub manager(&rpc_client->getChannel());
        brpc::Controller * cntl = new brpc::Controller;
        call_ids.emplace_back(cntl->call_id());
        Protos::CancelQueryResponse * response = new Protos::CancelQueryResponse();
        request.set_query_id(query_id);
        request.set_coordinator_address(coordinator_addr);
        manager.cancelQuery(cntl, &request, response, brpc::NewCallback(OnCancelQueryCallback, response, cntl, rpc_client, query_id));
        LOG_INFO(log, "Cancel plan segment query_id-{} on host-{}", query_id, extractExchangeHostPort(addr));
    }
}

bool SegmentScheduler::finishPlanSegments(const String & query_id)
{

    std::unique_lock<std::mutex> lock(mutex);
    auto query_map_ite = query_map.find(query_id);
    if (query_map_ite != query_map.end())
    {
        query_map.erase(query_map_ite);
    }

    {
        std::unique_lock<std::mutex> lock(segment_profile_mutex);

        auto seg_profile_map_ite = segment_profile_map.find(query_id);
        if (seg_profile_map_ite != segment_profile_map.end())
            segment_profile_map.erase(seg_profile_map_ite);
    }

    {
        std::unique_lock<std::mutex> lock(segment_status_mutex);
        auto seg_status_map_ite = segment_status_map.find(query_id);
        if (seg_status_map_ite != segment_status_map.end())
            segment_status_map.erase(seg_status_map_ite);

        query_status_map.erase(query_id);

        query_to_exception_with_code.remove(query_id);
        query_status_received_counter_map.erase(query_id);
    }
    return true;
}

AddressInfos SegmentScheduler::getWorkerAddress(const String & query_id, size_t segment_id)
{
    std::unique_lock<std::mutex> lock(mutex);
    auto query_map_ite = query_map.find(query_id);
    if (query_map_ite == query_map.end())
        return {};
    std::shared_ptr<DAGGraph> dag_ptr = query_map_ite->second;
    if (dag_ptr->id_to_address.count(segment_id))
        return dag_ptr->id_to_address[segment_id];
    else
        return {};
}

void SegmentScheduler::updateQueryStatus(const RuntimeSegmentStatus & segment_status)
{
    std::unique_lock<std::mutex> lock(segment_status_mutex);
    auto query_iter = query_status_map.find(segment_status.query_id);
    if (query_iter == query_status_map.end())
        return;
    RuntimeSegmentsStatusPtr & status = query_iter->second;
    status->is_succeed &= segment_status.is_succeed;
    status->is_cancelled |= segment_status.is_cancelled;
    status->metrics.cpu_micros += segment_status.metrics.cpu_micros;
}

void SegmentScheduler::updateSegmentStatus(const RuntimeSegmentStatus & segment_status)
{
    std::unique_lock<std::mutex> lock(segment_status_mutex);
    auto segment_status_iter = segment_status_map.find(segment_status.query_id);
    if (segment_status_iter == segment_status_map.end())
        return;

    auto segment_iter = segment_status_iter->second.find(segment_status.segment_id);
    if (segment_iter == segment_status_iter->second.end())
        segment_status_iter->second[segment_status.segment_id] = std::make_shared<RuntimeSegmentStatus>();

    RuntimeSegmentsStatusPtr status = segment_status_iter->second[segment_status.segment_id];

    status->query_id = segment_status.query_id;
    status->segment_id = segment_status.segment_id;
    status->is_succeed = segment_status.is_succeed;
    status->is_cancelled = segment_status.is_cancelled;
    status->metrics.cpu_micros += segment_status.metrics.cpu_micros;
    status->message = segment_status.message;
    status->code = segment_status.code;
}


void SegmentScheduler::updateSegmentProfile(PlanSegmentProfilePtr & segment_profile)
{
    std::unique_lock<std::mutex> lock(segment_profile_mutex);
    auto segment_profile_iter = segment_profile_map.find(segment_profile->query_id);
    if (segment_profile_iter == segment_profile_map.end())
        return;

    PlanSegmentProfilePtr profile = segment_profile;
    segment_profile_iter->second[segment_profile->segment_id].emplace_back(profile);
}

std::unordered_map<size_t, PlanSegmentProfiles> SegmentScheduler::getSegmentsProfile(const String & query_id)
{
    std::unordered_map<size_t, PlanSegmentProfiles> res;
    {
        std::unique_lock<std::mutex> lock(segment_profile_mutex);
        auto segment_profile_iter = segment_profile_map.find(query_id);
        if (segment_profile_iter == segment_profile_map.end())
            return res;
        res = segment_profile_iter->second;
    }
    return res;
}

void SegmentScheduler::checkQueryCpuTime(const String & query_id)
{
    UInt64 max_cpu_seconds = 0;
    OverflowMode overflow_mode = OverflowMode::THROW;

    std::unique_lock<std::mutex> lock(mutex);
    auto query_map_ite = query_map.find(query_id);
    if (query_map_ite == query_map.end())
    {
        LOG_INFO(log, "query_id-{} is not exist in scheduler query map", query_id);
        return;
    }

    // get limit settings from final segemnt.
    std::shared_ptr<DAGGraph> dag_ptr = query_map_ite->second;
    if (dag_ptr == nullptr)
        return;
    ContextPtr final_segment_context = dag_ptr->getContext();
    if (final_segment_context)
    {
        auto & settings = final_segment_context->getOptimizerContext()->getSettingsRef();
        max_cpu_seconds = settings.max_distributed_query_cpu_seconds;
        overflow_mode = final_segment_context->getSettingsRef().timeout_overflow_mode;
    }

    if (max_cpu_seconds <= 0)
        return;

    std::unique_lock<std::mutex> status_lock(segment_status_mutex);
    UInt64 total_cpu_micros = 0;
    auto query_iter = query_status_map.find(query_id);
    if (query_iter != query_status_map.end())
    {
        total_cpu_micros = query_status_map[query_id]->metrics.cpu_micros;
    }

    LOG_TRACE(log, "DistributedQuery total CpuTime-{} / {}", total_cpu_micros * 1.0 / 1000000, max_cpu_seconds);

    if (total_cpu_micros > max_cpu_seconds * 1000000)
    {
        switch (overflow_mode)
        {
            case OverflowMode::THROW:
                throw Exception(ErrorCodes::QUERY_CPU_TIMEOUT_EXCEEDED,
                    "Timeout exceeded: distribute cpu time {} seconds, maximum: {}",
                    static_cast<double>(total_cpu_micros * 1.0 / 1000000), static_cast<double>(max_cpu_seconds));
            case OverflowMode::BREAK:
                break;
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: unknown overflow mode");
        }
    }
}

void SegmentScheduler::updateReceivedSegmentStatusCounter(const String & query_id, const size_t & segment_id, const UInt64 & parallel_index)
{
    std::shared_ptr<DAGGraph> dag_ptr;
    {
        std::unique_lock<std::mutex> lock(mutex);
        auto all_segments_iterator = query_map.find(query_id);
        if (all_segments_iterator == query_map.end())
        {
            LOG_INFO(log, "query_id-{} is not exist in scheduler query map", query_id);
            return;
        }

        dag_ptr = all_segments_iterator->second;

        if (dag_ptr == nullptr)
            return;
    }

    if (dag_ptr->getContext()->getOptimizerContext()->isExplainQuery())
    {
        bool all_received = true;
        {
            // update counter and return
            std::unique_lock<std::mutex> lock(segment_status_mutex);
            auto segment_status_counter_iterator = query_status_received_counter_map[query_id].find(segment_id);
            if (segment_status_counter_iterator == query_status_received_counter_map[query_id].end())
            {
                query_status_received_counter_map[query_id][segment_id] = {};
            }
            query_status_received_counter_map[query_id][segment_id].insert(parallel_index);

            for (auto & parallel : dag_ptr->segment_parallel_size_map)
            {
                if (parallel.first == 0)
                    continue;

                if (query_status_received_counter_map[query_id][parallel.first].size() < parallel.second)
                {
                    all_received = false;
                }
            }
        }

        if (all_received)
        {
            ProfileLogHub<ProcessorProfileLogElement>::getInstance().stopConsume(query_id);
            LOG_DEBUG(log, "Query:{} have received all segment status.", query_id);
        }
    }
}

bool SegmentScheduler::alreadyReceivedAllSegmentStatus(const String & query_id)
{
    std::unique_lock<std::mutex> lock(segment_status_mutex);
    auto all_segments_iterator = query_map.find(query_id);
    auto received_status_segments_counter_iterator = query_status_received_counter_map.find(query_id);
    if (received_status_segments_counter_iterator == query_status_received_counter_map.end() && all_segments_iterator == query_map.end())
        return true;
    if (received_status_segments_counter_iterator == query_status_received_counter_map.end())
        return false;
    if (all_segments_iterator == query_map.end())
        return true;
    auto dag_ptr = all_segments_iterator->second;
    auto received_status_segments_counter = received_status_segments_counter_iterator->second;
    for (auto & parallel : dag_ptr->segment_parallel_size_map)
    {
        if (parallel.first == 0)
            continue;

        if (query_status_received_counter_map[query_id][parallel.first].size() < parallel.second)
            return false;
    }
    return true;
}

void SegmentScheduler::onSegmentFinished(const RuntimeSegmentStatus & /*status*/)
{
}

void SegmentScheduler::buildDAGGraph(PlanSegmentTree * plan_segments_ptr, std::shared_ptr<DAGGraph> graph_ptr)
{
    graph_ptr->plan_segment_status_ptr = std::make_shared<PlanSegmentsStatus>();
    PlanSegmentTree::Nodes & nodes = plan_segments_ptr->getNodes();

    if (nodes.size() <= 1)
    {
        graph_ptr->plan_segment_status_ptr->is_final_stage_start = true;
        return;
    }

    // use to traversal the tree
    std::stack<PlanSegmentTree::Node *> plan_segment_stack;
    std::vector<PlanSegment *> plan_segment_vector;
    std::set<size_t> plan_segment_vector_id_list;
    for (PlanSegmentTree::Node & node : nodes)
    {
        plan_segment_stack.emplace(&node);
        if (plan_segment_vector_id_list.find(node.getPlanSegment()->getPlanSegmentId()) == plan_segment_vector_id_list.end())
        {
            plan_segment_vector.emplace_back(node.getPlanSegment());
            plan_segment_vector_id_list.emplace(node.getPlanSegment()->getPlanSegmentId());
        }
    }
    while (!plan_segment_stack.empty())
    {
        PlanSegmentTree::Node * node_ptr = plan_segment_stack.top();
        plan_segment_stack.pop();
        for (PlanSegmentTree::Node * node : node_ptr->children)
        {
            plan_segment_stack.emplace(node);
            if (plan_segment_vector_id_list.find(node->getPlanSegment()->getPlanSegmentId()) == plan_segment_vector_id_list.end())
            {
                plan_segment_vector.emplace_back(node->getPlanSegment());
                plan_segment_vector_id_list.emplace(node->getPlanSegment()->getPlanSegmentId());
            }
        }
    }

    for (PlanSegment * plan_segment_ptr : plan_segment_vector)
    {
        graph_ptr->id_to_segment.emplace(std::make_pair(plan_segment_ptr->getPlanSegmentId(), plan_segment_ptr));
        // value, readnothing, system table
        if (plan_segment_ptr->getPlanSegmentInputs().empty())
        {
            graph_ptr->leaf_segments.insert(plan_segment_ptr->getPlanSegmentId());
            // graph_ptr->segments_has_table_scan.insert(plan_segment_ptr->getPlanSegmentId());
        }
        // source
        if (!plan_segment_ptr->getPlanSegmentInputs().empty())
        {
            bool all_tables = true;
            bool any_tables = false;
            for (const auto & input : plan_segment_ptr->getPlanSegmentInputs())
            {
                if (input->getPlanSegmentType() != RIPlanSegment::SOURCE)
                    all_tables = false;
                if (input->getPlanSegmentType() == RIPlanSegment::SOURCE)
                    any_tables = true;
            }
            if (all_tables)
                graph_ptr->leaf_segments.insert(plan_segment_ptr->getPlanSegmentId());
            if (any_tables)
                graph_ptr->table_scan_or_value_segments.insert(plan_segment_ptr->getPlanSegmentId());
        }
        // final stage
        if (plan_segment_ptr->getPlanSegmentOutput()->getPlanSegmentType() == RIPlanSegment::OUTPUT)
        {
            if (graph_ptr->final != std::numeric_limits<size_t>::max())
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: PlanSegments should be only one final stage");
            }
            else
            {
                graph_ptr->final = plan_segment_ptr->getPlanSegmentId();
            }
        }
    }
    // set exchangeParallelSize for plan inputs
    for (PlanSegment * plan_segment_ptr : plan_segment_vector)
    {
        for (const auto & input : plan_segment_ptr->getPlanSegmentInputs())
        {
            if (input->getPlanSegmentType() == RIPlanSegment::EXCHANGE)
            {
                if (graph_ptr->id_to_segment.find(input->getPlanSegmentId()) == graph_ptr->id_to_segment.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Logical error: can't find the segment which id is {}", input->getPlanSegmentId());
                PlanSegment * input_plan_segment_ptr = graph_ptr->id_to_segment.find(input->getPlanSegmentId())->second;
                input->setExchangeParallelSize(input_plan_segment_ptr->getExchangeParallelSize());
            }
        }
    }
    // do some check
    // 1. check if leaf segments or the final is empty
    if (graph_ptr->leaf_segments.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: no leaf segment");
    if (graph_ptr->final == std::numeric_limits<size_t>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: final is empty");

    // 2. check the parallel size
    for (auto it = graph_ptr->id_to_segment.begin(); it != graph_ptr->id_to_segment.end(); it++)
    {
        if (!it->second->getPlanSegmentInputs().empty())
        {
            for (const auto & plan_segment_input_ptr : it->second->getPlanSegmentInputs())
            {
                // only check when input is from an another exchange
                if (plan_segment_input_ptr->getPlanSegmentType() != RIPlanSegment::EXCHANGE)
                    continue;
                size_t input_plan_segment_id = plan_segment_input_ptr->getPlanSegmentId();
                if (graph_ptr->id_to_segment.find(input_plan_segment_id) == graph_ptr->id_to_segment.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Logical error: can't find the segment which id is {}",input_plan_segment_id);
                auto & input_plan_segment_ptr = graph_ptr->id_to_segment.find(input_plan_segment_id)->second;
                for (auto & plan_segment_output : input_plan_segment_ptr->getPlanSegmentOutputs())
                {
                    if (plan_segment_output->getExchangeId() != plan_segment_input_ptr->getExchangeId())
                        continue;
                    graph_ptr->exchanges[plan_segment_input_ptr->getExchangeId()] = {plan_segment_input_ptr, plan_segment_output};
                    // if stage out is write to local:
                    // 1.the left table for broadcast join
                    // 2.the left table or right table for local join
                    // the next stage parallel size must be the same
                    if (plan_segment_output->getExchangeMode() == RExchangeMode::LOCAL_NO_NEED_REPARTITION
                        || plan_segment_output->getExchangeMode() == RExchangeMode::LOCAL_MAY_NEED_REPARTITION)
                    {
                        if (input_plan_segment_ptr->getParallelSize() != it->second->getParallelSize())
                            throw Exception(ErrorCodes::LOGICAL_ERROR, 
                                "Logical error: the parallel size between local stage is different, input id:{} current id:{}"
                                , it->second->getPlanSegmentId(), input_plan_segment_id);
                        plan_segment_output->setParallelSize(1);
                    }
                    else
                    {
                        // if stage out is shuffle, the output parallel size must be equal to next stage parallel size
                        if (plan_segment_output->getParallelSize() != it->second->getParallelSize())
                        {
                            throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Logical error: the parallel size between stage is different, input id:{} current id:{}"
                                    , input_plan_segment_id, it->second->getPlanSegmentId());
                        }
                    }
                }
            }
        }
        graph_ptr->segment_parallel_size_map.emplace(it->first, it->second->getParallelSize());
    }
}

PlanSegmentExecutionInfo
SegmentScheduler::scheduleV2(const String & query_id, ContextPtr query_context, std::shared_ptr<DAGGraph> dag_graph_ptr)
{
    Stopwatch sw;
    PlanSegmentExecutionInfo execution_info;
    try
    {
        //TODO: lizhuoyu, need cluster name, maybe we need a config about it
        auto cluster_name = query_context->getClusters().begin()->first;
        auto scheduler = std::make_shared<MPPScheduler>(
            query_id,
            cluster_name,
            query_context,
            dag_graph_ptr,
            query_context->getOptimizerContext()->getSettingsRef().enable_batch_send_plan_segment);
        
        execution_info = scheduler->schedule();
    }
    catch (const Exception & e)
    {
        this->cancelPlanSegments(
            query_id,
            ErrorCodes::LOGICAL_ERROR,
            "receive exception during scheduler:" + e.message(),
            "coordinator",
            query_context,
            dag_graph_ptr);
        e.rethrow();
    }
    catch (...)
    {
        this->cancelPlanSegments(
            query_id, ErrorCodes::LOGICAL_ERROR, "receive unknown exception during scheduler", "coordinator", query_context, dag_graph_ptr);
        throw;
    }
    sw.stop();
    ProfileEvents::increment(ProfileEvents::ScheduleTimeMilliseconds, sw.elapsedMilliseconds());
    return execution_info;
}

PlanSegmentSet SegmentScheduler::getIOPlanSegmentInstanceIDs(const String & query_id) const
{
    std::unique_lock<std::mutex> lock(mutex);
    auto iter = query_map.find(query_id);
    if (iter == query_map.end() || !iter->second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "query_id-{} does not exist in scheduler query map", query_id);
    const auto & dag_ptr = iter->second;
    PlanSegmentSet res;
    for (auto && segment_id : dag_ptr->table_scan_or_value_segments)
    {
        /// wont wait for final segment, because it is already logged in progress_callback
        if (segment_id != dag_ptr->final)
        {
            for (size_t parallel_id = 0; parallel_id < dag_ptr->segment_parallel_size_map[segment_id]; parallel_id++)
            {
                res.insert({static_cast<UInt32>(segment_id), static_cast<UInt32>(parallel_id)});
            }
        }
    }

    return res;
}

void SegmentScheduler::workerRestarted(const WorkerID & id, const HostWithPorts & /*host_ports*/, UInt32 register_time)
{
    // Is there any better solution than iteration?
    LOG_TRACE(log, "Worker {} with register time {} restarted, notify schedulers who care.", id.toString(), register_time);
}
}
