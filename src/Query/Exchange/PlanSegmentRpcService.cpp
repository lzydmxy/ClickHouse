#include "PlanSegmentRpcService.h"
#include <brpc/controller.h>
#include <butil/iobuf.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <IO/Progress.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <Access/AccessControl.h>
#include <Access/User.h>
#include <Query/Common/QueryCommon.h>
#include <Query/Common/OptimizerContext.h>
#include <Query/Common/OptimizerSettings.h>
#include <Query/ProtosHelper/AddressInfo.h>
#include <Query/ProtosHelper/ProgressHelper.h>
#include <Query/Executor/ProfileLogHub.h>
#include <Query/Executor/SegmentScheduler.h>
#include <Query/Executor/PlanSegmentInstance.h>
#include <Query/Executor/PlanSegmentReport.h>
#include <Query/Executor/executePlanSegment.h>
#include <Query/Exchange/bRPC/ReadBufferFromBrpc.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BRPC_PROTOCOL_VERSION_UNSUPPORT;
    extern const int QUERY_WAS_CANCELLED;
    extern const int QUERY_WAS_CANCELLED_INTERNAL;
    extern const int TIMEOUT_EXCEEDED;
    extern const int EPOCH_MISMATCH;
}

RepeatedTimerTask::RepeatedTimerTask(BackgroundSchedulePool &pool_, UInt64 interval_, const std::string& name_)
    : interval(interval_)
{
    task = pool_.createTask(name_, [this]{ run(); });
}

void ResourceMonitorTimer::updateResourceData() {
}

void ResourceMonitorTimer::run() {
    updateResourceData();
    task->scheduleAfter(interval);
}

void PlanSegmentRpcService::cancelQuery(
    ::google::protobuf::RpcController * controller,
    const RCancelQueryRequest * request,
    RCancelQueryResponse * response,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    brpc::Controller * cntl = static_cast<brpc::Controller *>(controller);

    try
    {
        auto cancel_code = optimizer_context->getPlanSegmentProcessList()->tryCancelPlanSegmentGroup(
            request->query_id(), request->coordinator_address());
        response->set_status_code(std::to_string(static_cast<int>(cancel_code)));
    }
    catch (...)
    {
        auto error_msg = getCurrentExceptionMessage(false);
        cntl->SetFailed(error_msg);
        LOG_ERROR(log, "Cancel query failed: {}", error_msg);
    }
}

void PlanSegmentRpcService::reportPlanSegmentStatus(
    ::google::protobuf::RpcController * controller,
    const RPlanSegmentStatusRequest * request,
    RPlanSegmentStatusResponse * /*response*/,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    brpc::Controller * cntl = static_cast<brpc::Controller *>(controller);
    LOG_DEBUG(
        log,
        "Received status of query {}, segment {}, parallel index {}, succeed: {}, cancelled: {}, status code is {}",
        request->query_id(),
        request->segment_id(),
        request->parallel_index(),
        request->is_succeed(),
        request->is_canceled(),
        request->status_code());
    try
    {
        bool is_cancelled = (request->status_code() == ErrorCodes::QUERY_WAS_CANCELLED_INTERNAL) ||
            (request->status_code() == ErrorCodes::QUERY_WAS_CANCELLED);
        RuntimeSegmentStatus status{
            request->query_id(),
            request->segment_id(),
            request->parallel_index(),
            request->attempt_id(),
            request->is_succeed(),
            is_cancelled,
            RuntimeSegmentsMetrics(request->metrics()),
            request->message(),
            request->status_code()};

        SegmentSchedulerPtr scheduler = optimizer_context->getSegmentScheduler();
        scheduler->updateSegmentStatus(status);
        scheduler->updateQueryStatus(status);
        scheduler->updateReceivedSegmentStatusCounter(request->query_id(), request->segment_id(), request->parallel_index());
        if (!status.is_cancelled && status.code == 0)
        {
            try
            {
                scheduler->checkQueryCpuTime(status.query_id);
            }
            catch (const Exception & e)
            {
                status.message = e.message();
                status.code = e.code();
                status.is_succeed = false;
            }
        }

        // this means exception happened during execution.
        auto coordinator = QueryMPPManager::instance().getCoordinator(request->query_id());
        if (coordinator && request->metrics().has_progress())
        {
            Progress progress = ProgressHelper::progressFromProto(status.metrics.final_progress);
            coordinator->onFinalProgress(request->segment_id(), request->parallel_index(), progress);
        }
        if (!status.is_succeed)
        {
            if (coordinator)
                coordinator->updateSegmentInstanceStatus(status);
            else
            {
                LOG_INFO(
                    log,
                    "can't find coordinator for query_id:{} segment_id:{} parallel_index:{}",
                    request->query_id(),
                    request->segment_id(),
                    request->parallel_index());
            }
            scheduler->onSegmentFinished(status);
        }
        // todo  scheduler.cancelSchedule
    }
    catch (...)
    {
        auto error_msg = getCurrentExceptionMessage(false);
        cntl->SetFailed(error_msg);
        LOG_ERROR(log, "sendPlanSegmentStatus failed: {}", error_msg);
    }
}

void PlanSegmentRpcService::reportPlanSegmentProfile(
    ::google::protobuf::RpcController * /*controller*/,
    const RPlanSegmentProfileRequest * request,
    RPlanSegmentProfileResponse * /*response*/,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    PlanSegmentProfilePtr profile = PlanSegmentProfile::fromProto(*request);
    const SegmentSchedulerPtr & scheduler = optimizer_context->getSegmentScheduler();
    scheduler->updateSegmentProfile(profile);
}

void PlanSegmentRpcService::prepareCommonParams(
    UInt32 major_revision,
    UInt32 query_common_buf_size,
    UInt32 query_settings_buf_size,
    brpc::Controller * cntl,
    RQueryCommonPtr & query_common,
    SettingsChangesPtr & settings_changes)
{
    if (major_revision != DBMS_BRPC_PROTOCOL_MAJOR_VERSION)
        throw Exception(ErrorCodes::BRPC_PROTOCOL_VERSION_UNSUPPORT,
            "brpc protocol major version different - current is {} remote is {}, plan segment is not compatible",
                major_revision, DBMS_BRPC_PROTOCOL_MAJOR_VERSION);
    /// Prepare query_common.
    butil::IOBuf query_common_buf;
    auto query_common_buf_size_act = cntl->request_attachment().cutn(&query_common_buf, query_common_buf_size);
    if (query_common_buf_size_act != query_common_buf_size)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Impossible query_common_buf_size_act: {} expected: {}", query_common_buf_size_act, query_common_buf_size);
    }

    butil::IOBufAsZeroCopyInputStream wrapper(query_common_buf);
    bool res = query_common->ParseFromZeroCopyStream(&wrapper);
    if (!res)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Fail to parse Protos::QueryCommon!");

    /// Prepare settings.
    butil::IOBuf settings_common_buf;
    if (query_settings_buf_size > 0)
    {
        auto query_settings_buf_size_act = cntl->request_attachment().cutn(&settings_common_buf, query_settings_buf_size);
        if (query_settings_buf_size_act != query_settings_buf_size)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Impossible query_settings_buf_size_act: {} expected: {}", query_settings_buf_size_act, query_settings_buf_size);
        }
    }
    auto settings_io_buf = std::make_shared<butil::IOBuf>(settings_common_buf.movable());
    if (!settings_io_buf->empty())
    {
        /// apply settings changed
        ReadBufferFromBrpc settings_read_buf(*settings_io_buf);
        Settings settings;
        OptimizerSettings optimizer_settings;
        const size_t MIN_MINOR_VERSION_ENABLE_STRINGS_WITH_FLAGS = 4;
        if (query_common->brpc_minor_revision() >= MIN_MINOR_VERSION_ENABLE_STRINGS_WITH_FLAGS)
            settings.read(settings_read_buf, SettingsWriteFormat::STRINGS_WITH_FLAGS);
        else
            settings.read(settings_read_buf, SettingsWriteFormat::BINARY);
        if (query_common->brpc_minor_revision() >= MIN_MINOR_VERSION_ENABLE_STRINGS_WITH_FLAGS)
            optimizer_settings.read(settings_read_buf, SettingsWriteFormat::STRINGS_WITH_FLAGS);
        else
            optimizer_settings.read(settings_read_buf, SettingsWriteFormat::BINARY);
        auto changes = settings.changes();
        auto optimizer_changes = optimizer_settings.changes();
        changes.insert(changes.end(), optimizer_changes.begin(), optimizer_changes.end());
        settings_changes = std::make_shared<SettingsChanges>(changes);
    }
}

ContextMutablePtr PlanSegmentRpcService::createQueryContext(
    ContextMutablePtr global_context,
    RQueryCommonPtr & query_common,
    UInt16 remote_side_port,
    PlanSegmentInstanceID instance_id)
{
    /// Create context.
    ContextMutablePtr query_context;
    /// Create session context for worker such as ClusterProxy/executeQuery/updateSettingsForCluster function
    query_context = Context::createCopy(global_context);
    auto optimizer_context = query_context->getOptimizerContext();
    auto address = std::make_shared<AddressInfo>();
    address->fromProto(query_common->coordinator_address());
    optimizer_context->setCoordinatorAddress(address);
    optimizer_context->setPlanSegmentInstanceID(instance_id);

    /// Authentication
    Poco::Net::SocketAddress initial_socket_address(query_common->initial_client_host(), query_common->initial_client_port());
    Poco::Net::SocketAddress current_socket_address(query_common->coordinator_address().host_name(), remote_side_port);
    Decimal64 initial_query_start_time_microseconds{query_common->initial_query_start_time()};

    /// Set client info.
    ClientInfo & client_info = query_context->getClientInfo();
    //client_info.brpc_protocol_minor_version = query_common->brpc_protocol_minor_revision();
    client_info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
    client_info.interface = ClientInfo::Interface::BRPC;
    client_info.initial_query_start_time = initial_query_start_time_microseconds / 1000000;
    client_info.initial_query_start_time_microseconds = initial_query_start_time_microseconds;
    client_info.initial_user = query_common->initial_user();
    client_info.initial_query_id = query_common->query_id();
    client_info.initial_address = std::move(initial_socket_address);
    client_info.current_query_id = client_info.initial_query_id + "_" + std::to_string(instance_id.segment_id);
    client_info.current_address = std::move(current_socket_address);
    //client_info.rpc_port = query_common->coordinator_address().exchange_port();
    query_context->setInternalQuery(query_common->is_internal_query());

    return query_context;
}

UUID getDefinerID(DB::ContextPtr context, String user)
{
    const auto & access_control = context->getAccessControl();
    return access_control.getID<DB::User>(user);
}

void PlanSegmentRpcService::initQueryContext(
    ContextMutablePtr query_context,
    RQueryCommonPtr query_common,
    SettingsChangesPtr settings_changes,
    const AddressInfo & execution_address)
{
    /// Authentication
    const auto & current_user = execution_address.getUser();    
    //query_context->setUser(current_user, execution_address.getPassword(), query_context->getClientInfo().current_address);
    query_context->setUser(getDefinerID(query_context, current_user));

    /// apply settings changed, must after setUser
    if (settings_changes)
        query_context->applySettingsChanges(*settings_changes);

    /// Disable function name normalization when it's a secondary query, because queries are either
    /// already normalized on initiator node, or not normalized and should remain unnormalized for
    /// compatibility.
    query_context->setSetting("normalize_function_names", Field(0));

    //TODO: Need grant access and set quota
    //query_context->grantAllAccess();
    //query_context->setQuotaKey(query_common->quota());

    if (!query_context->hasQueryContext())
        query_context->makeQueryContext();

    query_context->getOptimizerContext()->initQueryExpirationTimeStamp();
}

void PlanSegmentRpcService::innerExecutePlanSegment(
    RQueryCommonPtr query_common,
    SettingsChangesPtr settings_changes,
    UInt16 remote_side_port,
    UInt32 segment_id,
    PlanSegmentExecutionInfo & execution_info,
    std::shared_ptr<butil::IOBuf> plan_segment_buf,
    PlanSegmentProcessList::EntryPtr process_plan_segment_entry,
    ContextMutablePtr query_context)
{
    LOG_INFO(log, "execute plan segment: {}_{}, parallel index {}", query_common->query_id(), segment_id, execution_info.parallel_id);

    if (!query_context)
        query_context = createQueryContext(context, query_common, remote_side_port, {segment_id, execution_info.parallel_id});

    initQueryContext(query_context, query_common, settings_changes, *execution_info.execution_address);
    query_context->getOptimizerContext()->setLogsQueue(optimizer_context->getLogsQueue());
    ThreadFromGlobalPool async_thread([query_common = std::move(query_common),
                                       settings_changes = std::move(settings_changes),
                                       segment_id = segment_id,
                                       execution_info = std::move(execution_info),
                                       plan_segment_buf = std::move(plan_segment_buf),
                                       process_plan_segment_entry = std::move(process_plan_segment_entry),
                                       query_context = std::move(query_context)]() mutable {
        bool before_execute = true;
        try
        {
            auto optimizer_context = query_context->getOptimizerContext();

            if (!process_plan_segment_entry)
                process_plan_segment_entry = optimizer_context->getPlanSegmentProcessList()->insertGroup(query_context, segment_id);

            process_plan_segment_entry->prepareQueryScope(query_context);

            /// Plan segment Deserialization can't run in bthread since checkStackSize method is not compatible with all user-space lightweight threads that manually allocated stacks.
            butil::IOBufAsZeroCopyInputStream plansegment_buf_wrapper(*plan_segment_buf);
            RPlanSegment plan_segment_proto;
            plan_segment_proto.ParseFromZeroCopyStream(&plansegment_buf_wrapper);
            // copy some commnon field from query_common;
            plan_segment_proto.set_query_id(query_common->query_id());
            plan_segment_proto.set_segment_id(segment_id);
            plan_segment_proto.mutable_coordinator_address()->CopyFrom(query_common->coordinator_address());
            auto plan_segment = std::make_unique<PlanSegment>();
            plan_segment->fromProto(plan_segment_proto, query_context);
            plan_segment->update(query_context);
            auto segment_instance = std::make_unique<PlanSegmentInstance>();

            before_execute = false;
            segment_instance->info = std::move(execution_info);
            segment_instance->plan_segment = std::move(plan_segment);
            // keep query_context for log query_id when exception
            executePlanSegmentInternal(std::move(segment_instance), query_context, std::move(process_plan_segment_entry), false);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__, query_context ? query_context->getCurrentQueryId() : "");
            if (before_execute && query_context)
            {
                int exception_code = getCurrentExceptionCode();
                auto exception_message = getCurrentExceptionMessage(false);

                auto result = convertFailurePlanSegmentStatusToResult(std::move(query_context), execution_info, exception_code, exception_message);
                reportExecutionResult(result);
            }
        }
    });
    async_thread.detach();
}

void PlanSegmentRpcService::executePlanSegment(
    ::google::protobuf::RpcController * controller,
    const RPlanSegmentRequest * request,
    RPlanSegmentResponse * response,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    brpc::Controller * cntl = static_cast<brpc::Controller *>(controller);
    try
    {
        auto query_common = std::make_shared<RQueryCommon>();
        SettingsChangesPtr settings_changes;
        prepareCommonParams(
            request->brpc_major_revision(),
            request->query_common_buf_size(),
            request->query_settings_buf_size(),
            cntl,
            query_common,
            settings_changes);

        PlanSegmentExecutionInfo execution_info;

        execution_info.execution_address = std::make_shared<AddressInfo>(request->execution_address());
        execution_info.parallel_id = request->parallel_id();
        execution_info.source_task_filter.fromProto(request->source_task_filter());
        execution_info.attempt_id = request->attempt_id();

        if (request->sources_size() != 0)
        {
            for (const auto & s : request->sources())
            {
                PlanSegmentPartitionSource source;
                source.fromProto(s);
                execution_info.sources[source.exchange_id].emplace_back(std::move(source));
            }
        }

        butil::IOBuf plan_segment_buf;
        auto plan_segment_buf_size = cntl->request_attachment().cutn(&plan_segment_buf, request->plan_segment_buf_size());
        if (plan_segment_buf_size != request->plan_segment_buf_size())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Impossible plan_segment_buf_size: {} expected: {}",
                plan_segment_buf_size, request->plan_segment_buf_size());
        }

        innerExecutePlanSegment(
            std::move(query_common),
            std::move(settings_changes),
            cntl->remote_side().port,
            request->plan_segment_id(),
            execution_info,
            std::make_shared<butil::IOBuf>(plan_segment_buf.movable()));
        //report_metrics_timer->getResourceData().fillProto(*response->mutable_worker_resource_data());
        //LOG_TRACE(log, "adaptive scheduler worker status: {}", response->worker_resource_data().ShortDebugString());
    }
    catch (...)
    {
        auto error_msg = getCurrentExceptionMessage(true);
        cntl->SetFailed(error_msg);
        LOG_ERROR(log, "executeQuery failed: {}", error_msg);
    }
}

void PlanSegmentRpcService::executePlanSegments(
    ::google::protobuf::RpcController * controller,
    const RPlanSegmentsRequest * request,
    RPlanSegmentResponse * response,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    brpc::Controller * cntl = static_cast<brpc::Controller *>(controller);
    try
    {
        const auto & headers = request->headers();
        auto query_common = std::make_shared<RQueryCommon>();
        SettingsChangesPtr settings_changes;
        prepareCommonParams(
            request->brpc_major_revision(),
            request->query_common_buf_size(),
            request->query_settings_buf_size(),
            cntl,
            query_common,
            settings_changes);

        // prepare segmentGroup
        std::vector<size_t> segment_ids;
        std::optional<PlanSegmentInstanceID> first_instance_id;
        for (const auto & header : headers)
        {
            segment_ids.emplace_back(header.plan_segment_id());
            if (!first_instance_id)
                first_instance_id = {header.plan_segment_id(), header.parallel_id()};
        }

        auto execution_address = std::make_shared<AddressInfo>(request->execution_address());
        auto first_query_context
            = createQueryContext(context, query_common, cntl->remote_side().port, *first_instance_id);
        auto optimizer_context = first_query_context->getOptimizerContext();
        auto process_plan_segment_entries = optimizer_context->getPlanSegmentProcessList()->insertGroup(first_query_context, segment_ids);

        for (int i = 0; i < headers.size(); i++)
        {
            const auto & header = headers[i];
            PlanSegmentExecutionInfo execution_info;
            execution_info.parallel_id = header.parallel_id();
            execution_info.execution_address = execution_address;
            execution_info.attempt_id = header.attempt_id();
            execution_info.source_task_filter.fromProto(header.source_task_filter());
            butil::IOBuf plan_segment_buf;
            auto plan_segment_buf_size = cntl->request_attachment().cutn(&plan_segment_buf, header.plan_segment_buf_size());
            if (plan_segment_buf_size != header.plan_segment_buf_size())
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Impossible plan_segment_buf_size: {} expected: {}", 
                    plan_segment_buf_size, header.plan_segment_buf_size());
            }

            innerExecutePlanSegment(
                query_common,
                settings_changes,
                cntl->remote_side().port,
                header.plan_segment_id(),
                execution_info,
                std::make_shared<butil::IOBuf>(plan_segment_buf.movable()),
                std::move(process_plan_segment_entries[i]),
                (i == 0) ? first_query_context : nullptr);
        }

        // report_metrics_timer->getResourceData().fillProto(*response->mutable_worker_resource_data());
        // LOG_TRACE(log, "adaptive scheduler worker status: {}", response->worker_resource_data().ShortDebugString());
    }
    catch (...)
    {
        auto error_msg = getCurrentExceptionMessage(true);
        cntl->SetFailed(error_msg);
        LOG_ERROR(log, "executeQuery failed: {}", error_msg);
    }
}

void PlanSegmentRpcService::executeProgress(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::ProgressRequest * request,
    ::DB::Protos::ProgressResponse * /*response*/,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    try
    {
        auto progress = ProgressHelper::progressFromProto(request->progress());
        auto coordinator = QueryMPPManager::instance().getCoordinator(request->query_id());
        if (coordinator)
        {
            coordinator->onProgress(request->segment_id(), request->parallel_id(), progress);
        }
        else
            LOG_INFO(log, "sendProgress cant find coordinator for query_id:{}", request->query_id());
    }
    catch (...)
    {
        auto error_msg = getCurrentExceptionMessage(true);
        controller->SetFailed(error_msg);
        LOG_ERROR(log, "sendProgress failed: {}", error_msg);
    }
}

void parseProcessorProfileRequest(ProcessorProfileLogElement & profile_log, const ::DB::Protos::ProcessorProfileRequest * request)
{
    profile_log.query_id = request->query_id();
    profile_log.event_time = request->event_time();
    profile_log.event_time_microseconds = request->event_time_microseconds();
    profile_log.elapsed_us = request->elapsed_us();
    profile_log.input_wait_elapsed_us = request->input_wait_elapsed_us();
    profile_log.output_wait_elapsed_us = request->output_wait_elapsed_us();
    profile_log.id = request->id();
    profile_log.input_rows = request->input_rows();
    profile_log.input_bytes = request->input_bytes();
    profile_log.output_rows = request->output_rows();
    profile_log.output_bytes = request->output_bytes();
    profile_log.processor_name = request->processor_name();
    profile_log.plan_group = request->plan_group();
    profile_log.plan_step = request->plan_step();
    // ProcessorProfileLogElement does not have the following fields
    //profile_log.step_id = request->step_id();
    //profile_log.worker_address = request->worker_address();
    //profile_log.parent_ids = std::vector<UInt64>(request->parent_ids().begin(), request->parent_ids().end());
}

void PlanSegmentRpcService::reportProcessorProfile(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::ProcessorProfileRequest * request,
    ::DB::Protos::ProcessorProfileResponse * /*response*/,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    ProcessorProfileLogElement element;
    try
    {
        parseProcessorProfileRequest(element, request);
        auto query_id = element.query_id;
        auto timeout = context->getOptimizerContext()->getSettingsRef().push_queue_timeout_millseconds;

        if (ProfileLogHub<ProcessorProfileLogElement>::getInstance().hasConsumer())
            ProfileLogHub<ProcessorProfileLogElement>::getInstance().tryPushElement(query_id, element, timeout);
    }
    catch (...)
    {
        auto error_msg = getCurrentExceptionMessage(true);
        controller->SetFailed(error_msg);
        LOG_ERROR(log, "reportProcessorProfileMetrics failed: {}", error_msg);
    }
}

void PlanSegmentRpcService::reportProcessorsProfile(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::ProcessorsProfileRequest * request,
    ::DB::Protos::ProcessorProfileResponse * /*response*/,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    std::vector<ProcessorProfileLogElement> elements;
    try
    {
        const auto & query_id = request->query_id();
        for (const auto & inner_request : request->request())
        {
            ProcessorProfileLogElement element;
            parseProcessorProfileRequest(element, &inner_request);
            elements.emplace_back(std::move(element));
        }
        auto timeout = context->getOptimizerContext()->getSettingsRef().push_queue_timeout_millseconds;
        if (ProfileLogHub<ProcessorProfileLogElement>::getInstance().hasConsumer())
            ProfileLogHub<ProcessorProfileLogElement>::getInstance().tryPushElement(query_id, elements, timeout);
    }
    catch (...)
    {
        auto error_msg = getCurrentExceptionMessage(true);
        controller->SetFailed(error_msg);
        LOG_ERROR(log, "batchReportProcessorProfileMetrics failed: {}", error_msg);
    }
}

void PlanSegmentRpcService::sendLogs(
    ::google::protobuf::RpcController * controller,
    const RSendLogsRequest * request,
    RSendLogsResponse * response,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    brpc::Controller * cntl = static_cast<brpc::Controller *>(controller);

    try
    {
        LOG_DEBUG(log, "Received logs from worker: {}, query_id: {}, log count: {},", request->worker_address(), request->query_id(), request->logs_size());

        // Add logs to the internal logs queue
        if (auto log_queue = optimizer_context->getLogsQueue())
        {
            // Convert protobuf logs to Block format
            Block log_block = InternalTextLogsQueue::getSampleBlock();
            ProtosSerDerHelper::fillFromProto(log_block, *request);
            log_queue->pushBlock(std::move(log_block));
        }
        else
        {
            LOG_WARNING(log, "No logs queue available to store received logs");
        }
    }
    catch (...)
    {
        auto error_msg = getCurrentExceptionMessage(true);
        cntl->SetFailed(error_msg);
        LOG_ERROR(log, "sendLogs failed: {}", error_msg);
    }
}

}
