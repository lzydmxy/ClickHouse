#include "PlanSegmentExecutor.h"
#include <boost/algorithm/string.hpp>
#include <base/types.h>
#include <base/time.h>
#include <Common/MemoryTracker.h>
#include <Common/logger_useful.h>
#include <QueryPipeline/BlockIO.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/Transforms/CopyTransform.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Query/Common/OptimizerContext.h>
#include <Query/Common/SystemLogHelper.h>
#include <Query/ProtosHelper/QueryProto.h>
#include <Query/ProtosHelper/RPCHelpers.h>
#include <Query/Transforms/BufferedCopyTransform.h>
#include <Query/Exchange/RpcChannelPool.h>
#include <Query/Exchange/ExchangeUtils.h>
#include <Query/Exchange/RepartitionTransform.h>
#include <Query/Exchange/SinglePartitionExchangeSink.h>
#include <Query/Exchange/MultiPartitionExchangeSink.h>
#include <Query/Exchange/BroadcastExchangeSink.h>
#include <Query/Exchange/LoadBalancedExchangeSink.h>
#include <Query/Exchange/DataTrans/IBroadcastSender.h>
#include <Query/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Query/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <Query/Exchange/DataTrans/MultiPathReceiver.h>
#include <Query/Exchange/DataTrans/LocalBroadcastChannel.h>
#include <Query/Exchange/bRPC/AsyncRegisterResult.h>
#include <Query/Exchange/bRPC/BrpcRemoteBroadcastReceiver.h>
#include <Query/Processors/QueryPlan/BuildQueryPipelineSettingsExt.h>
#include <Query/Processors/Exchange/ExchangeSourceExt.h>
#include <Query/Executor/PlanSegmentReport.h>
#include <Query/Executor/RuntimeFilter/RuntimeFilterManager.h>
#include <Query/Processors/IQueryPlanStepExt.h>
#include <Query/Processors/QueryPlan/BuildQueryPipelineSettingsHelper.h>
#include <Query/ProtosHelper/ProgressHelper.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>

namespace ProfileEvents
{
    extern const Event SystemTimeMicroseconds;
    extern const Event UserTimeMicroseconds;
    extern const Event PlanSegmentInstanceRetry;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int EXCHANGE_DATA_TRANS_EXCEPTION;
    extern const int BSP_CLEANUP_PREVIOUS_SEGMENT_INSTANCE_FAILED;
    extern const int BSP_WRITE_DATA_FAILED;
}

/// Call this inside catch block.
void setExceptionStackTrace(QueryLogElement & elem)
{
    LockMemoryExceptionInThread lock(VariableContext::Global);
    try
    {
        throw;
    }
    catch (const std::exception & e)
    {
        elem.stack_trace = getExceptionStackTraceString(e);
    }
    catch (...)
    {
    }
}

void PlanSegmentExecutor::prepareSegmentInfo() const
{
    query_log_element->client_info = context->getClientInfo();
    query_log_element->segment_id = plan_segment->getPlanSegmentId();
    // query_log_element->segment_parallel = plan_segment->getParallelSize();
    // query_log_element->segment_parallel_index = plan_segment_instance->info.parallel_id;
    query_log_element->type = QueryLogElementType::QUERY_START;
    const auto time_now = std::chrono::system_clock::now();
    query_log_element->event_time = timeInSeconds(time_now);
    query_log_element->event_time_microseconds = timeInMicroseconds(time_now);
    query_log_element->query_start_time = query_log_element->event_time;
    query_log_element->query_start_time_microseconds = query_log_element->event_time_microseconds;
}

PlanSegmentExecutor::PlanSegmentExecutor(
    PlanSegmentInstancePtr plan_segment_instance_, ContextMutablePtr context_, PlanSegmentProcessList::EntryPtr process_plan_segment_entry_)
    : process_plan_segment_entry(std::move(process_plan_segment_entry_))
    , context(std::move(context_))
    , optimizer_context(context->getOptimizerContext())
    , plan_segment_instance(std::move(plan_segment_instance_))
    , plan_segment(plan_segment_instance->plan_segment.get())
    , plan_segment_outputs(plan_segment_instance->plan_segment->getPlanSegmentOutputs())
    , logger(getLogger("PlanSegmentExecutor"))
    , query_log_element(std::make_unique<QueryLogElementExt>())
{
    options = ExchangeUtils::getExchangeOptions(context);
    if (plan_segment->getPlanSegmentId() > 0)
        prepareSegmentInfo();
}

PlanSegmentExecutor::PlanSegmentExecutor(
    PlanSegmentInstancePtr plan_segment_instance_,
    ContextMutablePtr context_,
    PlanSegmentProcessList::EntryPtr process_plan_segment_entry_,
    ExchangeOptions options_)
    : process_plan_segment_entry(std::move(process_plan_segment_entry_))
    , context(std::move(context_))
    , optimizer_context(context->getOptimizerContext())
    , plan_segment_instance(std::move(plan_segment_instance_))
    , plan_segment(plan_segment_instance->plan_segment.get())
    , plan_segment_outputs(plan_segment_instance->plan_segment->getPlanSegmentOutputs())
    , options(std::move(options_))
    , logger(getLogger("PlanSegmentExecutor"))
    , query_log_element(std::make_unique<QueryLogElementExt>())
{
    if (plan_segment->getPlanSegmentId() > 0)
        prepareSegmentInfo();
}

PlanSegmentExecutor::~PlanSegmentExecutor() noexcept
{
    try
    {
        if (context->getSettingsRef().log_queries && !context->isInternalQuery()
            && query_log_element->type >= context->getSettingsRef().log_queries_min_type && plan_segment->getPlanSegmentId() > 0)
        {
            if (auto query_log = context->getQueryLog())
                query_log->add(*query_log_element);
        }
    }
    catch (...)
    {
        LOG_ERROR(logger, "QueryLogElement:[query_id-{}] save to table fail with exception:{}",
            query_log_element->client_info.initial_query_id,
            getCurrentExceptionCode());
    }

    for (const auto & id : plan_segment->getRuntimeFilters())
        RuntimeFilterManager::getInstance().removeDynamicValue(plan_segment->getQueryId(), id);
}

std::optional<PlanSegmentExecutor::ExecutionResult> PlanSegmentExecutor::execute()
{
    auto send_logs_level = context->getSettingsRef().send_logs_level;
    if (send_logs_level != LogsLevel::none)
    {
        if (context->getOptimizerContext()->getLogsQueue())
            CurrentThread::attachInternalTextLogsQueue(context->getOptimizerContext()->getLogsQueue(), send_logs_level);

        auto current_address = getLocalAddress(context);
        auto coordinator_address = plan_segment->getCoordinatorAddress();
        if (current_address != coordinator_address)
        {
            logs_queue = std::make_shared<InternalTextLogsQueue>();
            logs_queue->max_priority = Poco::Logger::parseLevel(send_logs_level.toString());
            logs_queue->setSourceRegexp(context->getSettingsRef().send_logs_source_regexp);
            CurrentThread::attachInternalTextLogsQueue(logs_queue, send_logs_level);
        }
    }

    LOG_DEBUG(logger, "Execute planSegment:[\n{}\n]", plan_segment->toString());

    try
    {
        /// Remove normalized_query_plan_hash code, see normalized_query_hash
        context->getOptimizerContext()->initPlanSegmentExceptionHandler();
        doExecute();

        query_log_element->type = QueryLogElementType::QUERY_FINISH;
        const auto finish_time = std::chrono::system_clock::now();
        query_log_element->event_time = timeInSeconds(finish_time);
        query_log_element->event_time_microseconds = timeInMicroseconds(finish_time);

        sendLogs();

        return convertSuccessPlanSegmentStatusToResult(
            context, plan_segment_instance->info, final_progress, sender_metrics, plan_segment_outputs, segment_profile);
    }
    catch (...)
    {
        int exception_code = getCurrentExceptionCode();
        auto exception_message = getCurrentExceptionMessage(false);

        query_log_element->type = QueryLogElementType::EXCEPTION_WHILE_PROCESSING;
        query_log_element->exception_code = exception_code;
        query_log_element->exception = exception_message;
        if (context->getSettingsRef().calculate_text_stack_trace && exception_code != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
            setExceptionStackTrace(*query_log_element);
        const auto time_now = std::chrono::system_clock::now();
        query_log_element->event_time = timeInSeconds(time_now);
        query_log_element->event_time_microseconds = timeInMicroseconds(time_now);

        if (exception_code == ErrorCodes::MEMORY_LIMIT_EXCEEDED)
        {
            // ErrorCodes::MEMORY_LIMIT_EXCEEDED don't print stack trace.
            LOG_ERROR(logger, " [{}_{}] Query has excpetion with code: {}, msg: {}",
                plan_segment->getQueryId(), plan_segment->getPlanSegmentId(), exception_code, exception_message);
        }
        else
        {
            tryLogCurrentException(logger, fmt::format("[{}_{}]: Query has excpetion with code: {}, detail \n",
                    plan_segment->getQueryId(), plan_segment->getPlanSegmentId(), exception_code));
        }
        /// exception_handler will report failure plan segment status before release
        auto exception_handler = context->getOptimizerContext()->getPlanSegmentExceptionHandler();
        if (exception_handler && exception_handler->setException(std::current_exception()))
            return convertFailurePlanSegmentStatusToResult(context, plan_segment_instance->info, exception_code, exception_message,
                std::move(final_progress), sender_metrics, plan_segment_outputs);
        return {};
    }
}

BlockIO PlanSegmentExecutor::lazyExecute(bool /*add_output_processors*/)
{
    LOG_DEBUG(logger, "lazyExecute: {}", plan_segment->getPlanSegmentId());
    BlockIO res;
    // Will run as master query and already initialized
    if (!CurrentThread::get().getQueryContext() || CurrentThread::get().getQueryContext().get() != context.get())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Context not match");

    auto plan_segment_process_entry = optimizer_context->getPlanSegmentProcessList()->insertGroup(context, plan_segment->getPlanSegmentId());
    optimizer_context->getPlanSegmentProcessList()->insertProcessList(plan_segment_process_entry, plan_segment->getPlanSegmentId(), context);
    // set entry before buildPipeline to control memory usage of exchange queue
    optimizer_context->setPlanSegmentProcessListEntry(plan_segment_process_entry);
    res.pipeline = buildPipeline();
    return res;
}

void PlanSegmentExecutor::collectSegmentQueryRuntimeMetric(const QueryStatus * query_status)
{
    auto query_status_info = query_status->getInfo(true, context->getSettingsRef().log_profile_events);
    const auto & query_access_info = context->getQueryAccessInfo();

    query_log_element->read_bytes = query_status_info.read_bytes;
    query_log_element->read_rows = query_status_info.read_rows;
    query_log_element->written_bytes = query_status_info.written_bytes;
    query_log_element->written_rows = query_status_info.written_rows;
    query_log_element->memory_usage = query_status_info.peak_memory_usage > 0 ? query_status_info.peak_memory_usage : 0;
    query_log_element->query_duration_ms = query_status_info.elapsed_microseconds;
    query_log_element->thread_ids = std::move(query_status_info.thread_ids);
    query_log_element->profile_counters = query_status_info.profile_counters;
    query_log_element->query_tables = query_access_info.tables;
}

StepProfiles collectStepRuntimeProfiles(const QueryPipelinePtr & pipeline)
{
    ProcessorProfiles profiles;
    for (const auto & processor : pipeline->getProcessors())
        profiles.push_back(std::make_shared<ProcessorProfile>(processor.get()));
    GroupedProcessorProfilePtr grouped_profiles = GroupedProcessorProfile::getGroupedProfiles(profiles);
    auto step_profile = GroupedProcessorProfile::aggregateOperatorProfileToStepLevel(grouped_profiles);
    AddressToStepProfile addr_to_step_profile;
    addr_to_step_profile["localhost"] = step_profile;
    return ProfileMetric::aggregateStepProfileBetweenWorkers(addr_to_step_profile);
}

void fillPlanSegmentProfile(
    PlanSegmentProfilePtr & segment_profile,
    const QueryPipeline & pipeline,
    RReportProfileType::Enum type,
    const QueryStatus * query_status,
    ContextPtr context,
    PlanSegment * plan_segment)
{
    auto current_address = getLocalAddress(context);
    segment_profile->worker_address = extractExchangeHostPort(current_address);
    if (query_status)
    {
        auto query_status_info = query_status->getInfo(true, context->getSettingsRef().log_profile_events);
        segment_profile->read_bytes = query_status_info.read_bytes;
        segment_profile->read_rows = query_status_info.read_rows;
        segment_profile->query_duration_ms = query_status_info.elapsed_microseconds;
    }

    if (type == RReportProfileType::Unspecified)
        return;
    ProcessorProfiles profiles;
    for (const auto & processor : pipeline.getProcessors())
        profiles.push_back(std::make_shared<ProcessorProfile>(processor.get()));
    GroupedProcessorProfilePtr grouped_profiles = GroupedProcessorProfile::getGroupedProfiles(profiles);
    if (type == RReportProfileType::QueryPipeline)
    {
        auto output_root = GroupedProcessorProfile::getOutputRoot(grouped_profiles);
        segment_profile->profile_root_id = output_root->id;
        segment_profile->profiles = GroupedProcessorProfile::getProfileMetricsFromOutputRoot(output_root);
    }
    else if (type == RReportProfileType::QueryPlanExt)
    {
        auto step_profile = GroupedProcessorProfile::aggregateOperatorProfileToStepLevel(grouped_profiles);
        for (auto & [step_id, profile] : step_profile)
            segment_profile->profiles.emplace(step_id, profile);

        auto & query_plan = plan_segment->getQueryPlan();
        std::vector<QueryPlan::Node *> nodes_to_process;
        nodes_to_process.push_back(query_plan.getRootNode());

        /// Index starts at 1
        size_t node_index = 0;
        /// Depth-first traversal
        while (!nodes_to_process.empty())
        {
            const auto * node_to_process = nodes_to_process.back();
            node_index++;
            nodes_to_process.pop_back();
            nodes_to_process.insert(nodes_to_process.end(), node_to_process->children.begin(), node_to_process->children.end());

            auto * step_ext = dynamic_cast<IQueryPlanStepExt*>(node_to_process->step.get());
            if (step_ext)
            {
                if (!step_ext->getAttributeDescriptions().empty() && segment_profile->profiles.contains(node_index))
                {
                    for (auto & att : step_ext->getAttributeDescriptions())
                    {
                        auto attribute_ptr = std::make_shared<RuntimeAttributeDescription>(att.second);
                        segment_profile->profiles.at(node_index)->attributes.emplace(att.first, attribute_ptr);
                    }
                }
            }
        }
    }
}

void PlanSegmentExecutor::doExecute()
{
    SCOPE_EXIT_SAFE({
        if (context->getSettingsRef().log_queries && !context->isInternalQuery() && process_plan_segment_entry->getQueryStatus())
            collectSegmentQueryRuntimeMetric(process_plan_segment_entry->getQueryStatus().get());
    });

    optimizer_context->getPlanSegmentProcessList()->insertProcessList(process_plan_segment_entry, plan_segment->getPlanSegmentId(), context);
    optimizer_context->setPlanSegmentProcessListEntry(process_plan_segment_entry);
    auto query_status = process_plan_segment_entry->getQueryStatus();
    context->setProcessListElement(query_status);
    BroadcastSenderPtrs senders;
    auto pipeline = buildPipeline(senders);

    pipeline.setProcessListElement(query_status);
    pipeline.setProgressCallback([&, ctx_progress_callback = context->getProgressCallback()](const Progress & value) {
        if (ctx_progress_callback)
            ctx_progress_callback(value);
        this->progress.incrementPiecewiseAtomically(value);
        this->final_progress.incrementPiecewiseAtomically(value);
    });

    size_t max_threads = context->getSettingsRef().max_threads;
    if (max_threads)
        pipeline.setNumThreads(max_threads);
    size_t num_threads = pipeline.getNumThreads();

    PipelineExecutorPtr pipeline_executor;
    auto interactive_delay_opt = optimizer_context->getSettingsRef().interactive_delay_optimizer_mode;

    LOG_DEBUG(logger, "Execute query id {} segment id {} pipeline with {} threads, processor size {}, iteractive delay {}",
        plan_segment->getQueryId(), plan_segment->getPlanSegmentId(), num_threads, pipeline.getProcessors().size(), interactive_delay_opt);

    if (interactive_delay_opt == 0)
    {
        pipeline_executor = std::make_shared<PipelineExecutor>(pipeline.processors, pipeline.process_list_element);
        auto concurrency_control = pipeline.getConcurrencyControl();
        pipeline_executor->execute(num_threads, concurrency_control);
    }
    else
    {
        /// TODO: Because sink transformer is added to the buildPipeline method of PlanSegmentExecutor,
        /// There is no outputs, so PullingAsyncPipelineExecutor cannot be used,
        /// It requires at least one output in CK 24.3 version, but it is not required in 21.6
        LOG_TRACE(logger, "Execute pipeline aync, interactive_delay_optimizer_mode {}", interactive_delay_opt);
        PullingAsyncPipelineExecutor async_pipeline_executor(pipeline);
        Stopwatch after_send_progress;
        Block block;
        while (async_pipeline_executor.pull(block, interactive_delay_opt / 1000))
        {
            if (after_send_progress.elapsed() / 1000 >= context->getSettingsRef().interactive_delay)
            {
                /// Some time passed and there is a progress.
                after_send_progress.restart();
                sendProgress();
            }
        }
        // pipeline_executor = async_pipeline_executor.getPipelineExecutor();
    }
    // pipeline.setWriteCacheComplete(context);

    if (CurrentThread::getGroup())
    {
        metrics.cpu_micros = CurrentThread::getGroup()->performance_counters[ProfileEvents::SystemTimeMicroseconds]
                + CurrentThread::getGroup()->performance_counters[ProfileEvents::UserTimeMicroseconds];
    }

    //TODO: Print pipeline with GraphvizPrinter
    //pipeline_executor = async_pipeline_executor.getPipelineExecutor();
    // GraphvizPrinter::printPipeline(pipeline_executor->getProcessors(), pipeline_executor->getExecutingGraph(), 
    //     context, plan_segment->getPlanSegmentId(), extractExchangeHostPort(plan_segment_instance->info.execution_address));
    for (const auto & sender : senders)
    {
        auto status = sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "Upstream pipeline finished");
        /// bsp mode will fsync data in finish, so we need to check if exception is thrown here.
        if (context->getOptimizerContext()->getSettingsRef().bsp_mode && status.code != BroadcastStatusCode::ALL_SENDERS_DONE)
            throw Exception(ErrorCodes::BSP_WRITE_DATA_FAILED, "Write data into disk failed in bsp mode, code {}, error message: {}",
                status.code, status.message);
    }

    //TODO: Need PlanSegmentDescription in PlanPrinter.h
    // if (optimizer_context->getSettingsRef().log_segment_profiles)
    // {
    //     query_log_element->segment_profiles = std::make_shared<std::vector<String>>();
    //     query_log_element->segment_profiles->emplace_back(
    //         PlanSegmentDescription::getPlanSegmentDescription(plan_segment_instance->plan_segment, true)
    //             ->jsonPlanSegmentDescriptionAsString(collectStepRuntimeProfiles(pipeline)));
    // }

    if (optimizer_context->getSettingsRef().report_segment_profiles && plan_segment)
    {
        segment_profile = std::make_shared<PlanSegmentProfile>(query_log_element->client_info.initial_query_id, plan_segment->getPlanSegmentId());
        fillPlanSegmentProfile(segment_profile, pipeline, plan_segment->getProfileType(), query_status.get(), context, plan_segment);
    }

    if (context->getSettingsRef().log_processors_profiles)
    {
        auto processors_profile_log = context->getProcessorsProfileLog();
        if (!processors_profile_log)
            return;
        ProcessorProfileLogElement processor_profile_log;
        SystemLogHelper::addProcessorsProfileLog(processors_profile_log, &pipeline, context->getClientInfo().initial_query_id,
            std::chrono::system_clock::now(), plan_segment->getPlanSegmentId());
    }
}

static QueryPlanOptimizationSettings buildOptimizationSettingsWithCheck(LoggerPtr log, ContextMutablePtr& context)
{
    QueryPlanOptimizationSettings settings = QueryPlanOptimizationSettings::fromContext(context);
    return settings;
}

QueryPipeline PlanSegmentExecutor::buildPipeline()
{
    auto builder = plan_segment->getQueryPlan().buildQueryPipeline(
        buildOptimizationSettingsWithCheck(logger, context),
        BuildQueryPipelineSettingsExt::fromContext(context));

    //todo: need check ,not add by dev_opt
    BuildQueryPipelineSettingsHelper::fromPlanSegmentExt(plan_segment, plan_segment_instance->info, context, false);
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    registerAllExchangeReceivers(logger, pipeline, optimizer_context->getSettingsRef().exchange_wait_accept_max_timeout_ms);
    return pipeline;
}

QueryPipeline PlanSegmentExecutor::buildPipeline(BroadcastSenderPtrs & senders)
{
    std::vector<BroadcastSenderPtrs> senders_list;
    const auto opt_settings = context->getOptimizerContext()->getSettingsRef();
    auto sender_options
        = SenderProxyOptions{.wait_timeout_ms = opt_settings.exchange_wait_accept_max_timeout_ms + opt_settings.wait_runtime_filter_timeout};
    auto & sender_registry = BroadcastSenderProxyRegistry::instance();
    auto thread_group = CurrentThread::getGroup();
    UInt64 query_tx_id = optimizer_context->getTransactionID(context->getInitialQueryId());

    size_t output_index = 0;
    for (const auto &cur_plan_segment_output : plan_segment_outputs)
    {
        size_t exchange_parallel_size = cur_plan_segment_output->getExchangeParallelSize();
        size_t parallel_size = cur_plan_segment_output->getParallelSize();
        auto exchange_mode = cur_plan_segment_output->getExchangeMode();
        size_t exchange_id = cur_plan_segment_output->getExchangeId();
        const Block & header = cur_plan_segment_output->getHeader();
        BroadcastSenderPtrs current_exchange_senders;

        if (exchange_mode == RExchangeMode::BROADCAST)
            exchange_parallel_size = 1;

        /// output partitions num = num of plan_segment * exchange size
        /// for example, if downstream plansegment size is 2 (parallel_id is 0 and 1) and exchange_parallel_size is 4
        /// Exchange Sink will repartition data into 8 partition(2*4), partition id is range from 0 to 7.
        /// downstream plansegment and consumed partitions table:
        /// plansegment parallel_id :  partition id
        /// -----------------------------------------------
        /// 0                       : 0,1,2,3
        /// 1                       : 4,5,6,7
        size_t total_partition_num = exchange_parallel_size == 0 ? parallel_size : parallel_size * exchange_parallel_size;

        if (total_partition_num == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Total partition number should not be zero");

        for (size_t i = 0; i < total_partition_num; i++)
        {
            size_t partition_id = i;
            auto data_key = std::make_shared<ExchangeDataKey>(query_tx_id, exchange_id, partition_id);

            LOG_TRACE(logger, "output index {}, query_tx_id {}, exchange id {}, partition_id {}",
                output_index, query_tx_id, exchange_id, partition_id);

            BroadcastSenderPtr sender;
            auto proxy = sender_registry.getOrCreate(data_key, sender_options);
            proxy->accept(context, header);
            sender = proxy;
            // LOG_TRACE(logger, "buildPipeline query_tx_id {}, exchange_id {}, partition id {}, sender {}"
            //     , query_tx_id, exchange_id, partition_id, *data_key);
            current_exchange_senders.emplace_back(std::move(sender));
        }
        senders_list.emplace_back(std::move(current_exchange_senders));

        LOG_TRACE(logger, "Add sender, query_tx_id {} output index {} total_partition_num {}, exchange_parallel_size {}, parallel_size {}, sender list size {}"
            , query_tx_id, output_index, total_partition_num, exchange_parallel_size, parallel_size, senders_list.size());
        output_index ++;
    }

    auto builder = plan_segment->getQueryPlan().buildQueryPipeline(
        buildOptimizationSettingsWithCheck(logger, context),
        BuildQueryPipelineSettingsExt::fromContext(context));

    if (plan_segment->getPlanSegmentOutputs().empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PlanSegment has no output");

    size_t sink_num = 0;
    auto max_output_size = std::max(context->getSettingsRef().max_threads.value / plan_segment_outputs.size(), 1UL);
    auto output_size = optimizer_context->getSettingsRef().exchange_unordered_output_parallel_size.value;
    if (output_size > max_output_size)
        output_size = max_output_size;

    //TODO: Set max/min threads for pipeline or pipeline builder
    // pipeline->limitMinThreads(output_size * plan_segment_outputs.size());
    for (size_t i = 0; i < plan_segment_outputs.size(); ++i)
    {
        const auto &cur_plan_segment_output = plan_segment_outputs[i];
        const auto &current_exchange_senders = senders_list[i];
        auto exchange_mode = cur_plan_segment_output->getExchangeMode();
        bool keep_order = cur_plan_segment_output->needKeepOrder() || optimizer_context->getSettingsRef().exchange_enable_force_keep_order;

        switch (exchange_mode)
        {
            case RExchangeMode::REPARTITION:
            case RExchangeMode::LOCAL_MAY_NEED_REPARTITION:
            case RExchangeMode::GATHER:
            {
                size_t output_num = builder->getNumStreams();
                size_t partition_num = current_exchange_senders.size();
                bool need_resize =
                    keep_order
                    && optimizer_context->getSettingsRef().exchange_enable_keep_order_parallel_shuffle
                    && partition_num > 1;
                sink_num += (need_resize) ? output_num*partition_num : output_size;
                break;
            }
            case RExchangeMode::LOCAL_NO_NEED_REPARTITION:
            case RExchangeMode::BROADCAST:
                sink_num += output_size;
                break;
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find expected ExchangeMode {}", exchange_mode);
        }
    }
    LOG_TRACE(logger, "Build query_tx_id {} pipeline params : outputs size {}, senders {}, output parallel size {}, sink number {}"
        , query_tx_id, plan_segment_outputs.size(), senders_list.size(), output_size, sink_num);

    auto plan_segment_transform = [&](OutputPortRawPtrs ports)
    {
        Processors new_processors;
        std::vector<OutputPortRawPtrs> segs_output_ports(plan_segment_outputs.size(), OutputPortRawPtrs());
        /*
         * 1. initial pipeline state
         *
         *  _____     /------ ports[0]
         * |  T  | ---|------ ports[1]
         * |_____|    \------ ports[2]
         */
        if (plan_segment_outputs.size() > 1)
        {
            /*
             * 2.1. If plan segment has multi outputs, add copyTransfrom to pipeline.
             *
             *                                     _______________
             *            /------ ports[0] -------| CopyTransform1| ------------- ports[0] for plan_segment_outputs[0]
             *            |                       |_______________|        \----- ports[1] for plan_segment_outputs[1]
             *  _____     |                        _______________
             * |  T  | ---|------ ports[1] -------| CopyTransform2| ------------- ports[0] for plan_segment_outputs[0]
             * |_____|    |                       |_______________|        \----- ports[1] for plan_segment_outputs[1]
             *            |                        _______________
             *            \------ ports[2] -------| CopyTransform3| ------------- ports[0] for plan_segment_outputs[0]
             *                                    |_______________|        \----- ports[1] for plan_segment_outputs[1]
             *
             * Save the ports for plan_segment_outputs[0] in segs_output_ports[0],
             * Save the ports for plan_segment_outputs[1] in segs_output_ports[1]...
             */

            for (const auto & port : ports)
            {
                const auto & header = port->getHeader();
                auto copy_transform = std::make_shared<CopyTransform>(header, plan_segment_outputs.size());
                auto &copy_outputs = copy_transform->getOutputs();

                connect(*port, copy_transform->getInputs().front());

                size_t seg_id = 0;
                for (auto & copy_output : copy_outputs)
                {
                    segs_output_ports[seg_id].push_back(&copy_output);
                    ++seg_id;
                }
                new_processors.emplace_back(std::move(copy_transform));
            }
        }
        else
        {
            /*
             * 2.2. If there is only on plan_segment_output, than there is no need to add copyTransform.
             */
            segs_output_ports[0] = ports;
        }

        for (size_t i = 0; i < segs_output_ports.size(); ++i)
        {
            auto &cur_plan_segment_output = plan_segment_outputs[i];
            auto &current_exchange_senders = senders_list[i];
            auto exchange_mode = cur_plan_segment_output->getExchangeMode();
            bool keep_order = cur_plan_segment_output->needKeepOrder() || optimizer_context->getSettingsRef().exchange_enable_force_keep_order;
            const auto & header = segs_output_ports[i][0]->getHeader();

            // LOG_TRACE(logger, "transform seg output port index {}, keeper order {}, output size {}", i, keep_order, output_size);

            if (!keep_order && output_size)
            {
                /*
                 * 3.1. Add ResizeProcessor to pipeline.
                 *  _______________
                 * | CopyTransform1| ------------- ports[0] for plan_segment_outputs[0] -------------------------\
                 * |_______________|        \----- ports[1] for plan_segment_outputs[1]->ResizeProcessor 2       |
                 *  _______________                                                                              |
                 * | CopyTransform2| ------------- ports[0] for plan_segment_outputs[0] -------------------------|-------- ResizeProcessor 1, for seg 1
                 * |_______________|        \----- ports[1] for plan_segment_outputs[1]->ResizeProcessor 2       |         output ports num is output_size
                 *  _______________                                                                              |
                 * | CopyTransform3| ------------- ports[0] for plan_segment_outputs[0] -------------------------/
                 * |_______________|        \----- ports[1] for plan_segment_outputs[1]->ResizeProcessor 2
                 *
                 * If there is no CopyTransform, than pipeline will be:
                 *  _____     /------ ports[0] ------\
                 * |  T  | ---|------ ports[1] ------|-------ResizeProcessor 1, for seg 1
                 * |_____|    \------ ports[2] ------/       output ports num is output_size
                 */
                auto resize = std::make_shared<ResizeProcessor>(header, segs_output_ports[i].size(), output_size);
                auto &resize_inputs = resize->getInputs();
                auto &resize_outputs = resize->getOutputs();

                size_t input_index = 0;
                for (auto & input : resize_inputs)
                {
                    connect(*segs_output_ports[i][input_index], input);
                    ++input_index;
                }

                segs_output_ports[i].clear();
                for (auto & output : resize_outputs)
                {
                    segs_output_ports[i].emplace_back(&output);
                }

                new_processors.emplace_back(std::move(resize));

                // LOG_TRACE(logger, "transform seg output port index {}, resize input size {}, output size {}, new processors size {}",
                //     i, resize_inputs.size(), resize_outputs.size(), new_processors.size());
            }
            // else 3.2. No need to add ResizeProcessor.

            /* 4. Add ExchangeSink to pipeline. */
            Processors current_new_processors;

            switch (exchange_mode)
            {
                case RExchangeMode::REPARTITION:
                case RExchangeMode::LOCAL_MAY_NEED_REPARTITION:
                case RExchangeMode::GATHER:
                    current_new_processors = buildRepartitionExchangeSink(
                        current_exchange_senders, keep_order, i, header, segs_output_ports[i]);
                    break;
                case RExchangeMode::LOCAL_NO_NEED_REPARTITION:
                    current_new_processors = buildLoadBalancedExchangeSink(
                        current_exchange_senders, i, header, segs_output_ports[i]);
                    break;
                case RExchangeMode::BROADCAST:
                    current_new_processors = buildBroadcastExchangeSink(
                        current_exchange_senders, i, header, segs_output_ports[i]);
                    break;
                default:
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find expected ExchangeMode {}", exchange_mode);
            }

            new_processors.insert(new_processors.end(), current_new_processors.begin(), current_new_processors.end());

            // LOG_TRACE(logger, "transform seg output port index {}, ports size {}, exchange mode {}, current processor size {}, new processors size {}",
            //     i, segs_output_ports[i].size(), exchangeModeToString(exchange_mode), current_new_processors.size(), new_processors.size());
        }
        LOG_TRACE(logger, "Transform plan segment outputs size {}, ports size {}, new processors size {}"
            , plan_segment_outputs.size(), ports.size(), new_processors.size());
        return new_processors;
    };

    builder->transformExt(plan_segment_transform, sink_num, true);
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));

    // builder->init(*pipeline);
    // TODO: Set ChunkInfoTotals to Chunk in TotalsPortToMainPortTransform, It doesn't seem to work.
    // pipeline->setTotalsPortToMainPortTransform();
    // pipeline->setExtremesPortToMainPortTransform();

    auto timeout_ms = optimizer_context->getSettingsRef().exchange_wait_accept_max_timeout_ms;
    registerAllExchangeReceivers(logger, pipeline, timeout_ms);

    for (size_t i = 0; i < plan_segment_outputs.size(); ++i)
    {
        auto &current_exchange_senders = senders_list[i];
        for (auto &sender:current_exchange_senders)
        {
            senders.emplace_back(std::move(sender));
        }
    }

    if (senders.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Plan segment has no exchange sender!");

    return pipeline;
}

void PlanSegmentExecutor::registerAllExchangeReceivers(LoggerPtr log, const QueryPipeline & pipeline, UInt32 register_timeout_ms)
{
    const Processors & processors = pipeline.getProcessors();
    std::vector<AsyncRegisterResult> async_results;
    std::vector<LocalBroadcastChannel *> local_receivers;
    std::vector<MultiPathReceiver *> multi_receivers;
    std::exception_ptr exception;

    try
    {
        LOG_TRACE(log, "Register all exchangeReceivers processors size {}, time out {}", processors.size(), register_timeout_ms);

        for (const auto & processor : processors)
        {
            auto exchange_source_ptr = std::dynamic_pointer_cast<ExchangeSourceExt>(processor);
            if (!exchange_source_ptr)
                continue;

            auto * receiver_ptr = exchange_source_ptr->getReceiver().get();

            if (auto * brpc_receiver = dynamic_cast<BrpcRemoteBroadcastReceiver *>(receiver_ptr))
            {
                async_results.emplace_back(brpc_receiver->registerToSendersAsync(register_timeout_ms));
            }
            else if (auto * local_receiver = dynamic_cast<LocalBroadcastChannel *>(receiver_ptr))
            {
                local_receivers.push_back(local_receiver);
            }
            else if (auto * multi_receiver = dynamic_cast<MultiPathReceiver *>(receiver_ptr))
            {
                LOG_TRACE(log, "Register multi receiver {} to sender timeout {}", receiver_ptr->getName(), register_timeout_ms);
                multi_receiver->registerToSendersAsync(register_timeout_ms);
                multi_receivers.push_back(multi_receiver);
            }
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Register receivers, Unexpected SubReceiver Type: {}", typeid(receiver_ptr).name());
        }

        for (auto * receiver_ptr : local_receivers)
        {
            LOG_TRACE(log, "Register localhost {} to sender timeout {}", receiver_ptr->getName(), register_timeout_ms);
            receiver_ptr->registerToSenders(register_timeout_ms);
        }
        for (auto * receiver_ptr : multi_receivers)
            receiver_ptr->registerToLocalSenders(register_timeout_ms);
        for (auto * receiver_ptr : multi_receivers)
            receiver_ptr->registerToSendersJoin();
    }
    catch (...)
    {
        exception = std::current_exception();
    }

    // Wait all brpc register rpc done
    for (auto & res : async_results)
        brpc::Join(res.cntl->call_id());

    if (exception)
        std::rethrow_exception(std::move(exception));

    // get result
    for (auto & res : async_results)
    {
        // if exchange_enable_force_remote_mode = 1, sender and receiver in same process and sender stream may close before rpc end
        if (res.cntl->ErrorCode() == brpc::EREQUEST && boost::algorithm::ends_with(res.cntl->ErrorText(), "was closed before responded"))
        {
            LOG_INFO(log, "Receiver register sender successfully but sender already finished, host: {}, request: {}",
                butil::endpoint2str(res.cntl->remote_side()).c_str(), *res.request);
            continue;
        }
        res.channel->assertController(*res.cntl, ErrorCodes::EXCHANGE_DATA_TRANS_EXCEPTION);
        LOG_TRACE(log, "Receiver register sender successfully, host: {}, request: {}",
            butil::endpoint2str(res.cntl->remote_side()).c_str(), *res.request);
    }
}

Processors PlanSegmentExecutor::buildRepartitionExchangeSink(
    BroadcastSenderPtrs & senders, bool keep_order, size_t output_index, const Block &header, OutputPortRawPtrs &ports)
{
    Processors new_processors;

    ColumnsWithTypeAndName arguments;
    ColumnNumbers argument_numbers;
    for (const auto & column_name : plan_segment->getPlanSegmentOutput()->getShufflekeys())
    {
        arguments.emplace_back(plan_segment_outputs[output_index]->getHeader().getByName(column_name));
        argument_numbers.emplace_back(plan_segment_outputs[output_index]->getHeader().getPositionByName(column_name));
    }
    auto repartition_func = RepartitionTransform::getRepartitionHashFunction(
        plan_segment_outputs[output_index]->getShuffleFunctionName(),
        arguments,
        context,
        plan_segment_outputs[output_index]->getShuffleFunctionParams());
    size_t partition_num = senders.size();

    if (keep_order && optimizer_context->getSettingsRef().exchange_enable_keep_order_parallel_shuffle && partition_num > 1)
    {
        size_t output_num = ports.size();
        size_t sink_num = output_num * partition_num;

        new_processors.resize(1+output_num+sink_num);

        for (const auto & port : ports)
        {
            /* create one repartition transform for per port. */
            auto repartition_transform = std::make_shared<RepartitionTransform>(header, partition_num, argument_numbers, repartition_func);
            auto &repartition_outputs = repartition_transform->getOutputs();
            connect(*port, repartition_transform->getInputs().front());

            for (auto & repartition_output : repartition_outputs)
            {
                /* create BufferedCopyTransform, and connect RepartitionTransform(output port i) to BufferedCopyTransform */
                auto copy_transform = std::make_shared<BufferedCopyTransform>(header, partition_num, 20);
                connect(repartition_output, copy_transform->getInputPort());

                /* create SinglePartitionExchangeSink, and connect BufferedCopyTransform to SinglePartitionExchangeSink */
                auto & copy_outputs = copy_transform->getOutputs();
                size_t partition_id = 0;
                for (auto & copy_output : copy_outputs)
                {
                    String name = SinglePartitionExchangeSink::generateName(plan_segment_outputs[output_index]->getExchangeId());
                    auto exchange_sink =
                        std::make_shared<SinglePartitionExchangeSink>(header, senders[partition_id], partition_id, options, name);
                    connect(copy_output, exchange_sink->getPort());
                    new_processors.emplace_back(std::move(exchange_sink));

                    ++partition_id;
                }

                new_processors.emplace_back(std::move(copy_transform));
            }

            new_processors.emplace_back(repartition_transform);
        }
    }
    else
    {
        // size_t port_idx = 0;
        for (const auto & port : ports)
        {
            // port_idx ++;
            String name = MultiPartitionExchangeSink::generateName(plan_segment_outputs[output_index]->getExchangeId());
            auto exchange_sink =
                std::make_shared<MultiPartitionExchangeSink>(header, senders, repartition_func, argument_numbers, options, name);
            connect(*port, exchange_sink->getInputs().front());
            // LOG_TRACE(logger, "buildRepartitionExchangeSink, sink name {}, output index {}, port index {}, sink input size {}, output size {}",
            //     name, output_index, port_idx, exchange_sink->getInputs().size(), exchange_sink->getOutputs().size());
            new_processors.emplace_back(std::move(exchange_sink));
        }
    }

    LOG_TRACE(logger, "Output index {}, new processors size {}", output_index, new_processors.size());

    return new_processors;
}

Processors PlanSegmentExecutor::buildBroadcastExchangeSink(BroadcastSenderPtrs & senders, size_t output_index, const Block &header, OutputPortRawPtrs &ports)
{
    /// For broadcast exchange, we all 1:1 remote sender to one 1:N remote sender and can avoid duplicated serialization
    ExchangeUtils::mergeSenders(senders);
    LOG_DEBUG(logger, "After merge, broadcast sink size {}", senders.size());
    Processors new_processors;

    for (auto &port : ports)
    {
        String name = BroadcastExchangeSink::generateName(plan_segment_outputs[output_index]->getExchangeId());
        auto exchange_sink =
            std::make_shared<BroadcastExchangeSink>(header, senders, options, name);
        connect(*port, exchange_sink->getInputs().front());

        new_processors.emplace_back(std::move(exchange_sink));
    }

    return new_processors;
}

Processors PlanSegmentExecutor::buildLoadBalancedExchangeSink(BroadcastSenderPtrs & senders, size_t output_index, const Block &header, OutputPortRawPtrs &ports)
{
    Processors new_processors;

    for (auto &port : ports)
    {
        String name = LoadBalancedExchangeSink::generateName(plan_segment_outputs[output_index]->getExchangeId());
        auto exchange_sink =
            std::make_shared<LoadBalancedExchangeSink>(header, senders, name);
        connect(*port, exchange_sink->getInputs().front());

        new_processors.emplace_back(std::move(exchange_sink));
    }

    return new_processors;
}

void PlanSegmentExecutor::sendProgress()
{
    try
    {
        auto address = extractExchangeHostPort(plan_segment->getCoordinatorAddress());
        std::shared_ptr<RpcClient> rpc_client
            = RpcChannelPool::getInstance().getClient(address, BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY);
        Protos::PlanSegmentService_Stub manager(&rpc_client->getChannel());
        brpc::Controller * cntl = new brpc::Controller;
        auto * request = new RProgressRequest();
        auto * response = new RProgressResponse();
        request->set_query_id(plan_segment->getQueryId());
        request->set_segment_id(plan_segment->getPlanSegmentId());
        request->set_parallel_id(plan_segment_instance->info.parallel_id);
        *request->mutable_progress() = ProgressHelper::progressToProto(progress.fetchAndResetPiecewiseAtomically());
        cntl->set_timeout_ms(20000);

        std::function<String()> construct_err_msg = [request = request]() -> String {
            return fmt::format(
                "PlanSegment-{} send profile to coordinator failed, query id-{}", request->segment_id(), request->query_id());
        };

        manager.executeProgress(
            cntl,
            request,
            response,
            brpc::NewCallback(RPCHelpers::onAsyncCallDoneAssertController, request, response, cntl, logger, construct_err_msg));
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void PlanSegmentExecutor::sendLogs()
{
    try
    {
        if (!logs_queue)
            return;

        MutableColumns logs_columns;
        MutableColumns curr_logs_columns;
        size_t rows = 0;

        // Collect logs from queue (similar to TCPHandler::sendLogs)
        for (; logs_queue->tryPop(curr_logs_columns); ++rows)
        {
            if (rows == 0)
            {
                logs_columns = std::move(curr_logs_columns);
            }
            else
            {
                for (size_t j = 0; j < logs_columns.size(); ++j)
                    logs_columns[j]->insertRangeFrom(*curr_logs_columns[j], 0, curr_logs_columns[j]->size());
            }
        }
        if (rows > 0)
        {
            Block block = InternalTextLogsQueue::getSampleBlock();
            block.setColumns(std::move(logs_columns));

            LOG_DEBUG(logger, "Collected {} log entries from queue, sending to coordinator: {}", block.rows(), plan_segment->getCoordinatorAddress().getHostName());
            // Create RPC request
            std::shared_ptr<RpcClient> rpc_client = RpcChannelPool::getInstance().getClient(
                extractExchangeHostPort(plan_segment->getCoordinatorAddress()),
                BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY);

            RPlanSegmentServiceStub manager(&rpc_client->getChannel());
            brpc::Controller * cntl = new brpc::Controller;
            RSendLogsRequest * request = new RSendLogsRequest;
            RSendLogsResponse * response = new RSendLogsResponse;

            request->set_query_id(plan_segment->getQueryId());
            auto current_address = getLocalAddress(context);
            request->set_worker_address(extractExchangeHostPort(current_address));

            // Convert Block to protobuf format
            ProtosSerDerHelper::toProto(block, *request);
            cntl->set_timeout_ms(optimizer_context->getSettingsRef().send_plan_segment_timeout_ms.totalMilliseconds());

            std::function<String()> construct_err_msg = [request = request]() -> String {
                return fmt::format(
                    "Failed to send logs to coordinator {} for query {}", request->worker_address(), request->query_id());
            };

            manager.sendLogs(
                cntl,
                request,
                response,
                brpc::NewCallback(RPCHelpers::onAsyncCallDoneAssertController, request, response, cntl, logger, construct_err_msg));
        }
        else
        {
            LOG_DEBUG(logger, "No logs to send for query: {}", plan_segment->getQueryId());
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

}

}
