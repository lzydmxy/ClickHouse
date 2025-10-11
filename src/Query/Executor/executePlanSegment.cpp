#include "executePlanSegment.h"

#include <brpc/callback.h>
#include <brpc/controller.h>
#include <butil/iobuf.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ProcessList.h>
#include <Query/Common/OptimizerContext.h>
#include <Query/ProtosHelper/AddressInfo.h>
#include <Query/ProtosHelper/QueryProto.h>
#include <Query/Executor/PlanSegmentReport.h>
#include <Query/Executor/RuntimeFilter/RuntimeFilterManager.h>
#include <Query/Exchange/RpcChannelPool.h>
#include <Query/Executor/WorkerStatusManager.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

void AsyncContext::asyncComplete(brpc::CallId id, AsyncResult & async_result)
{
    std::unique_lock<std::mutex> lock(mutex);
    if (result.status == AsyncStats::FAILED)
        return;
    if (!async_result.is_success)
    {
        result.status = AsyncStats::FAILED;
        result.error_text = std::move(async_result.error_text);
        result.failed_worker = std::move(async_result.failed_worker);
        result.error_code = async_result.error_code;
        lock.unlock();
        cv.notify_all();
        return;
    }
    auto it = call_ids.find(id);
    if (it != call_ids.end())
    {
        if (call_ids.size() == 1)
        {
            //last callid, weak up main  thread
            result.status = AsyncContext::SUCCESS;
            call_ids.erase(it);
            lock.unlock();
            cv.notify_all();
            return;
        }
        else
            call_ids.erase(it);
    }   
}

void AsyncContext::addCallId(brpc::CallId id)
{
    std::unique_lock<std::mutex> lock(mutex);
    call_ids.emplace(id);
}

AsyncContext::AsyncResult AsyncContext::wait()
{
    std::unique_lock<std::mutex> lock(mutex);
    if (call_ids.size() == 0)
        return result;
    cv.wait(
        lock, [&] { return (result.status == AsyncStats::SUCCESS && call_ids.size() == 0) || result.status == AsyncStats::FAILED; });
    return result;
}

BlockIO executePlanSegmentClient(PlanSegmentInstancePtr plan_segment_instance, ContextMutablePtr context)
{
    if (!plan_segment_instance)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot execute empty plan segment");
    PlanSegmentExecutor executor(std::move(plan_segment_instance), std::move(context));
    return executor.lazyExecute();
}

void executePlanSegmentInternal(
    PlanSegmentInstancePtr plan_segment_instance,
    ContextMutablePtr context,
    PlanSegmentProcessList::EntryPtr process_plan_segment_entry,
    bool async)
{
    if (!plan_segment_instance)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot execute empty plan segment");

    const auto & opt_settings = context->getOptimizerContext()->getSettingsRef();
    if (opt_settings.query_dry_run_mode == QueryDryRunMode::SKIP_EXECUTE_SEGMENT)
        return;

    bool inform_success_status = opt_settings.enable_wait_for_post_processing || opt_settings.report_segment_profiles;
    auto executor = std::make_shared<PlanSegmentExecutor>(
        std::move(plan_segment_instance), std::move(context), std::move(process_plan_segment_entry));

    /// Because of CurrentThread::attachQueryForLog(query_) in ProcessList::insert() method, asynchronous execution is not supported
    if (async)
    {
        ThreadFromGlobalPool async_thread([executor_ = std::move(executor), inform_success_status_ = inform_success_status]() mutable {
            auto result = executor_->execute();
            executor_.reset(); /// release executor
            if (result)
                reportExecutionResult(*result, inform_success_status_);
        });
        async_thread.detach();
        return;
    }
    else
    {
        auto result = executor->execute();
        executor.reset(); /// release executor
        if (result)
            reportExecutionResult(*result, inform_success_status);
    }
}

static void OnSendPlanSegmentCallback(
    RPlanSegmentResponse * response,
    brpc::Controller * cntl,
    std::shared_ptr<RpcClient> rpc_channel,
    WorkerStatusManagerPtr worker_status_manager,
    AsyncContextPtr async_context,
    WorkerID worker_id)
{
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    std::unique_ptr<RPlanSegmentResponse> response_guard(response);

    if (worker_status_manager)
    {
        if (cntl->Failed())
            worker_status_manager->setWorkerNodeDead(worker_id, cntl->ErrorCode());
        else if (response->has_worker_resource_data())
            worker_status_manager->updateWorkerNode(response->worker_resource_data(), WorkerStatusManager::UpdateSource::ComeFromWorker);
    }
    rpc_channel->checkAliveWithController(*cntl);
    AsyncContext::AsyncResult result;
    if (cntl->Failed())
    {
        LOG_ERROR(
            getLogger("executePlanSegment"),
            "Send plansegment to {} failed, error: {},  msg: {}",
            butil::endpoint2str(cntl->remote_side()).c_str(),
            cntl->ErrorText(),
            response->message());
        result.error_text = cntl->ErrorText();
        result.error_code = cntl->ErrorCode();
        result.is_success = false;
        result.failed_worker = butil::endpoint2str(cntl->remote_side()).c_str();
        async_context->asyncComplete(cntl->call_id(), result);
    }
    else
    {
        LOG_TRACE(getLogger("executePlanSegment"), "Send plansegment to {} success , response {}", butil::endpoint2str(cntl->remote_side()).c_str(),response->ShortDebugString());
        async_context->asyncComplete(cntl->call_id(), result);
    }
}

void prepareQueryCommonBuf(
    butil::IOBuf & common_buf, const PlanSegment & any_plan_segment, ContextPtr & context)
{
    const auto opt_context = context->getOptimizerContext();
    const auto & opt_settings = opt_context->getSettingsRef();
    RQueryCommon query_common;
    const auto & client_info = context->getClientInfo();
    query_common.set_brpc_minor_revision(static_cast<UInt32>(DBMS_BRPC_PROTOCOL_MINOR_VERSION));
    query_common.set_query_id(any_plan_segment.getQueryId());
    query_common.set_initial_query_start_time(client_info.initial_query_start_time_microseconds.value);
    query_common.set_initial_user(client_info.initial_user);
    query_common.set_initial_client_host(client_info.initial_address.host().toString());
    query_common.set_initial_client_port(client_info.initial_address.port());
    any_plan_segment.getCoordinatorAddress().toProto(*query_common.mutable_coordinator_address());
    query_common.set_database(context->getCurrentDatabase());
    query_common.set_check_session(!opt_settings.enable_prune_source_plan_segment);
    auto query_expiration_ts = opt_context->getQueryExpirationTimeStamp();
    query_common.set_query_expiration_timestamp(timeInMilliseconds(query_expiration_ts));
    const String & quota_key = client_info.quota_key;
    if (!client_info.quota_key.empty())
        query_common.set_quota(quota_key);

    query_common.set_is_internal_query(context->isInternalQuery());

    // butil::IOBuf query_common_buf;
    butil::IOBufAsZeroCopyOutputStream wrapper(&common_buf);
    query_common.SerializeToZeroCopyStream(&wrapper);
}

void executePlanSegmentRemotelyWithPreparedBuf(
    size_t segment_id,
    PlanSegmentExecutionInfo execution_info,
    const butil::IOBuf & query_common_buf,
    const butil::IOBuf & query_settings_buf,
    const butil::IOBuf & plan_segment_buf,
    AsyncContextPtr & async_context,
    const Context & context,
    const WorkerID & worker_id)
{
    const auto opt_context = context.getOptimizerContext();
    const auto & opt_settings = opt_context->getSettingsRef();
    auto execute_address = extractExchangeHostPort(*execution_info.execution_address);
    auto rpc_channel = RpcChannelPool::getInstance().getClient(execute_address, BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY);
    Protos::PlanSegmentService_Stub manager_stub(&rpc_channel->getChannel());
    RPlanSegmentRequest request;
    request.set_brpc_major_revision(DBMS_BRPC_PROTOCOL_MAJOR_VERSION);
    request.set_parallel_id(execution_info.parallel_id);
    request.set_plan_segment_id(segment_id);
    request.set_attempt_id(execution_info.attempt_id);
    HostWithPorts::fillHostWithPorts(context.getOptimizerContext()->getHostWithPorts(), *request.mutable_coordinator_host_ports());
    if (execution_info.source_task_filter.isValid())
        *request.mutable_source_task_filter() = execution_info.source_task_filter.toProto();

    execution_info.execution_address->toProto(*request.mutable_execution_address());
    for (const auto & iter : execution_info.sources)
    {
        for (const auto & source : iter.second)
        {
            source.toProto(*request.add_sources());
        }
    }

    if (execution_info.worker_epoch > 0)
        request.set_worker_epoch(execution_info.worker_epoch);

    butil::IOBuf attachment;

    request.set_query_common_buf_size(query_common_buf.size());
    attachment.append(query_common_buf);

    request.set_query_settings_buf_size(query_settings_buf.size());
    if (!query_settings_buf.empty())
    {
        attachment.append(query_settings_buf);
    }

    request.set_plan_segment_buf_size(plan_segment_buf.size());
    attachment.append(plan_segment_buf);

    /// async call
    auto * cntl = new brpc::Controller();
    auto * response = new RPlanSegmentResponse();
    auto call_id = cntl->call_id();
    cntl->request_attachment().append(attachment.movable());
    cntl->set_timeout_ms(opt_settings.send_plan_segment_timeout_ms.totalMilliseconds());
    google::protobuf::Closure * done = brpc::NewCallback(
        &OnSendPlanSegmentCallback, response, cntl, std::move(rpc_channel), opt_context->getWorkerStatusManager(), async_context, worker_id);
    async_context->addCallId(call_id);
    manager_stub.executePlanSegment(cntl, &request, response, done);
}

void executePlanSegmentsRemotely(
    const AddressInfo & address_info,
    const PlanSegmentHeaders & plan_segment_headers,
    const butil::IOBuf & query_common_buf,
    const butil::IOBuf & query_settings_buf,
    AsyncContextPtr & async_context,
    const Context & context,
    const WorkerID & worker_id)
{
    const auto opt_context = context.getOptimizerContext();
    const auto & opt_settings = opt_context->getSettingsRef();
    auto execute_address = extractExchangeHostPort(address_info);
    auto rpc_channel = RpcChannelPool::getInstance().getClient(execute_address, BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY);
    Protos::PlanSegmentService_Stub manager_stub(&rpc_channel->getChannel());

    // common
    RPlanSegmentsRequest request;
    request.set_brpc_major_revision(DBMS_BRPC_PROTOCOL_MAJOR_VERSION);
    address_info.toProto(*request.mutable_execution_address());

    butil::IOBuf attachment;
    request.set_query_common_buf_size(query_common_buf.size());
    HostWithPorts::fillHostWithPorts(context.getOptimizerContext()->getHostWithPorts(), *request.mutable_coordinator_host_ports());
    attachment.append(query_common_buf);
    request.set_query_settings_buf_size(query_settings_buf.size());
    if (!query_settings_buf.empty())
        attachment.append(query_settings_buf);

    // private
    for (const auto & header : plan_segment_headers)
    {
        auto * proto = request.add_headers();
        header.toProto(*proto);
        attachment.append(*header.plan_segment_buf_ptr);
    }

    /// async call
    auto * response = new RPlanSegmentResponse();
    auto * cntl = new brpc::Controller();
    cntl->set_timeout_ms(opt_settings.send_plan_segment_timeout_ms.totalMilliseconds());
    auto call_id = cntl->call_id();
    cntl->request_attachment().append(attachment.movable());
    google::protobuf::Closure * done = brpc::NewCallback(
        &OnSendPlanSegmentCallback, response, cntl, std::move(rpc_channel), opt_context->getWorkerStatusManager(), async_context, worker_id);
    async_context->addCallId(call_id);
    manager_stub.executePlanSegments(cntl, &request, response, done);
}
}
