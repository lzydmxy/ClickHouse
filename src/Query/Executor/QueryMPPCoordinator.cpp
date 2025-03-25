#include "QueryMPPCoordinator.h"
#include <atomic>
#include <mutex>
#include <type_traits>
#include <Common/logger_useful.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <QueryPipeline/BlockIO.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ProcessList.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/Executor/QueryMPPManager.h>
#include <Query/Executor/PlanSegmentExecutor.h>
#include <Query/Executor/PlanSegmentInstance.h>
#include <Query/Executor/executePlanSegment.h>
#include <Query/Executor/SegmentScheduler.h>
#include <Query/Executor/RuntimeFilter/RuntimeFilterManager.h>
#include <Query/Executor/sendPlanSegment.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
}

static const Int16 AMBIGUOS_ERROR_MAX_NUM = 10;

QueryMPPCoordinator::QueryMPPCoordinator(
    const std::string cluster_name_, PlanSegmentTreeUniqPtr plan_segment_tree_, ContextMutablePtr query_context_, QueryMPPOptions options_)
    : cluster_name(cluster_name_)
    , plan_segment_tree(std::move(plan_segment_tree_))
    , query_context(std::move(query_context_))
    , optimizer_context(query_context->getOptimizerContext())
    , options(std::move(options_))
    , query_id(query_context->getClientInfo().current_query_id)
    , progress_manager(query_id)
    , log(getLogger("QueryMPPCoordinator"))
{
}

BlockIO QueryMPPCoordinator::execute()
{
    auto this_coordinator = shared_from_this();
    QueryMPPManager::instance().registerQuery(query_id, this_coordinator);

    PlanSegmentsStatusPtr scheduler_status;

    if (plan_segment_tree->getNodes().size() > 1)
    {
        RuntimeFilterManager::getInstance().registerQuery(query_id, *plan_segment_tree, query_context);
    }

    auto optimizer_context = query_context->getOptimizerContext();
    auto context_ptr = std::const_pointer_cast<const Context>(query_context);
    auto local_address = getLocalAddressPtr(context_ptr);
    optimizer_context->setCoordinatorAddress(local_address);
    optimizer_context->setPlanSegmentInstanceID(PlanSegmentInstanceID{0, 0});

    /// set progress_callback before send plan segment
    progress_manager.setProgressCallback([previous_progress_callback = query_context->getProgressCallback(),
                                          entry = optimizer_context->getProcessListEntry()](const Progress & p) {
        if (previous_progress_callback)
            previous_progress_callback(p);
        entry->getQueryStatus()->updateProgressIn(p);
    });

    {
        /// only send progress before executing final plan segment,
        /// working thread will join when this tcp progress sender is destroyed
        auto sender = std::make_unique<TCPProgressSender>(
            optimizer_context->getSendTCPProgress(), query_context->getSettingsRef().interactive_delay / 1000);
        scheduler_status = optimizer_context->getSegmentScheduler()->insertPlanSegments(query_id, plan_segment_tree.get(), query_context);
    }

    if (scheduler_status && !scheduler_status->exception.empty())
    {
        const auto error_msg = "Query failed before final task execution, error message:" + std::move(scheduler_status->exception);
        if (isAmbiguosError(scheduler_status->error_code))
        {
            auto status = waitUntilFinish(scheduler_status->error_code, error_msg);
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Execute error ambiguos code {}, message {}", status.error_code, status.summarized_error_msg);
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR,  "Execute error code {}, message {}", scheduler_status->error_code, error_msg);
    }

    if (!scheduler_status || !scheduler_status->is_final_stage_start)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get scheduler status from segment scheduler or final stage not started yet");
    }

    initializePostProcessingRPCReceived();

    auto * final_segment = plan_segment_tree->getRoot()->getPlanSegment();
    final_segment->update(query_context);
    LOG_TRACE(log, "EXECUTE: \n {}", final_segment->toString());

    auto final_segment_instance = std::make_unique<PlanSegmentInstance>();
    final_segment_instance->info = scheduler_status->final_execution_info;
    final_segment_instance->info.execution_address = local_address;
    final_segment_instance->plan_segment = std::make_unique<PlanSegment>(std::move(*final_segment));

    try
    {
        return DB::lazyExecutePlanSegmentLocally(std::move(final_segment_instance), query_context);
    }
    catch (const Exception & e)
    {
        if (isAmbiguosError(e.code()))
        {
            auto status = waitUntilFinish(e.code(), String(e.message()));
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Execute error code {}, message {}", status.error_code, status.summarized_error_msg);
        }
        throw;
    }
}

SummarizedQueryStatus QueryMPPCoordinator::waitUntilFinish(int error_code, const String & error_msg)
{
    std::unique_lock lock(status_mutex);
    if (status_cv.wait_for(lock, std::chrono::milliseconds(optimizer_context->getSettingsRef().distributed_query_wait_exception_ms), [this] {
            return this->query_status.status_code == QueryMPPStatusCode::FINISH;
        }))
    {
        if (query_status.success)
        {
            return SummarizedQueryStatus{.success = true};
        }
    }
    String summarized_error_msg;
    if (!query_status.root_cause_error.code)
    {

        query_status.root_cause_error = {.code = error_code, .message = error_msg};
        summarized_error_msg = fmt::format(
            "Query [{}] failed with RootCause: {}; \n AdditionalErrors: {} ",
            query_id,
            query_status.root_cause_error,
            fmt::join(query_status.additional_errors, "\n"));
    }
    else
    {
        summarized_error_msg = query_status.root_cause_error.message;
    }

    return SummarizedQueryStatus{
        .success = query_status.success,
        .cancelled = query_status.cancelled,
        .error_code = query_status.root_cause_error.code,
        .summarized_error_msg = std::move(summarized_error_msg)};
}

void QueryMPPCoordinator::updateSegmentInstanceStatus(const RuntimeSegmentStatus & status)
{
    LOG_TRACE(
        log,
        "updateSegmentInstanceStatus query_id:{} segment_id:{} is_succeed:{} is_cancelled:{} code:{} message:{}",
        query_id,
        status.segment_id,
        status.is_succeed,
        status.is_cancelled,
        status.code,
        status.message);
    if (status.is_succeed)
    {
        // Root query plan segment means this node is coordinator
        if (status.segment_id == 0)
        {
            finishQuery();
        }
    }
    else
    {
        QueryError query_error{.code = status.code, .message = status.message, .segment_id = status.segment_id};
        tryUpdateRootErrorCause(query_error, status.is_cancelled);
    }
}

void QueryMPPCoordinator::tryUpdateRootErrorCause(const QueryError & query_error, bool is_canceled)
{
    if (query_status.status_code.load(std::memory_order_acquire) == QueryMPPStatusCode::INIT)
    {
        cancelQuery(query_error, is_canceled);
    }

    std::unique_lock lock(status_mutex);
    if (query_status.success)
        return;

    if (isAmbiguosError(query_error.code))
    {
        if (query_status.additional_errors.size() < AMBIGUOS_ERROR_MAX_NUM)
            query_status.additional_errors.emplace_back(std::move(query_error));
        return;
    }

    if (!query_status.root_cause_error.code)
    {
        query_status.root_cause_error = std::move(query_error);
        lock.unlock();
        finishQuery();
        return;
    }
    query_status.additional_errors.emplace_back(std::move(query_error));
}

void QueryMPPCoordinator::onProgress(UInt32 segment_id, UInt32 parallel_index, const Progress & progress_)
{
    progress_manager.onProgress(segment_id, parallel_index, progress_);
}

void QueryMPPCoordinator::onFinalProgress(UInt32 segment_id, UInt32 parallel_index, const Progress & progress_)
{
    progress_manager.onFinalProgress(segment_id, parallel_index, progress_);
    if (optimizer_context->getSettingsRef().enable_wait_for_post_processing)
    {
        {
            std::unique_lock lock(post_processing_rpc_waiting_mutex);
            PlanSegmentInstanceID instance_id{segment_id, parallel_index};
            // save instance id in post_processing_rpc_waiting if not initialized
            if (post_processing_rpc_waiting_initialized)
            {
                post_processing_rpc_waiting[PostProcessingRPCID::ReportPlanSegmentCost].erase(instance_id);
            }
            else
            {
                post_processing_rpc_waiting[PostProcessingRPCID::ReportPlanSegmentCost].insert(instance_id);
            }
        }
        post_processing_rpc_waiting_cv.notify_all();
    }
}

Progress QueryMPPCoordinator::getFinalProgress() const
{
    return progress_manager.getFinalProgress();
}

void QueryMPPCoordinator::initializePostProcessingRPCReceived()
{
    if (optimizer_context->getSettingsRef().enable_wait_for_post_processing)
    {
        {
            std::unique_lock lock(post_processing_rpc_waiting_mutex);
            auto instance_ids = optimizer_context->getSegmentScheduler()->getIOPlanSegmentInstanceIDs(query_id);
            //remove instance id which has been received before
            for (auto instance_id : post_processing_rpc_waiting[PostProcessingRPCID::ReportPlanSegmentCost])
            {
                if (instance_ids.find(instance_id) != instance_ids.end())
                    instance_ids.erase(instance_id);
            }
            LOG_INFO(log, "initializePostProcessingRPCReceived query_id:{} with {} instances", query_id, instance_ids.size());
            post_processing_rpc_waiting[PostProcessingRPCID::ReportPlanSegmentCost] = std::move(instance_ids);
            post_processing_rpc_waiting_initialized = true;
        }
        post_processing_rpc_waiting_cv.notify_all();
    }
}

void QueryMPPCoordinator::waitUntilAllPostProcessingRPCReceived()
{
    // if setting is not enabled, just skip wait
    if (!optimizer_context->getSettingsRef().enable_wait_for_post_processing)
        return;
    std::unique_lock lock(post_processing_rpc_waiting_mutex);
    bool need_wait = false;

    if (!post_processing_rpc_waiting_initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "post_processing_rpc not initialized for query_id:{}", query_id);
    // need to wait if not all already received
    if (!post_processing_rpc_waiting[PostProcessingRPCID::ReportPlanSegmentCost].empty())
        need_wait = true;

    if (!need_wait)
    {
        LOG_TRACE(log, "waitUntilAllPostProcessingRPCReceived no need to wait");
        return;
    }

    if (!post_processing_rpc_waiting_cv.wait_for(
            lock, std::chrono::milliseconds(optimizer_context->getSettingsRef().wait_for_post_processing_timeout_ms), [this] {
                return post_processing_rpc_waiting[PostProcessingRPCID::ReportPlanSegmentCost].empty();
            }))
    {
        std::stringstream not_received_msg;
        for (auto instance_id : post_processing_rpc_waiting[PostProcessingRPCID::ReportPlanSegmentCost])
        {
            not_received_msg << instance_id.toString();
        }
        LOG_WARNING(log, "waitUntilAllPostProcessingRPCReceived failed for {} timeout, empty:{}", not_received_msg.str(),
            post_processing_rpc_waiting[PostProcessingRPCID::ReportPlanSegmentCost].empty());
    }
    LOG_TRACE(log, "waitUntilAllPostProcessingRPCReceived done");
}

void QueryMPPCoordinator::beginQuery()
{
    LOG_TRACE(log, "Begin execute query");
}

void QueryMPPCoordinator::cancelQuery(const QueryError & query_error, bool is_canceled)
{
    query_status.status_code.store(QueryMPPStatusCode::CANCEL, std::memory_order_release);
    LOG_TRACE(log, "Cancel execute query");
    optimizer_context->getPlanSegmentProcessList()->tryCancelPlanSegmentGroup(query_id);
    optimizer_context->getSegmentScheduler()->cancelPlanSegmentsFromCoordinator(
        query_id, query_error.code, query_error.message, query_context);
    if (!is_canceled)
    {
        query_status.status_code.store(QueryMPPStatusCode::WAIT_ROOT_ERROR, std::memory_order_release);
    }
}

void QueryMPPCoordinator::finishQuery()
{
    query_status.status_code.store(QueryMPPStatusCode::FINISH, std::memory_order_release);
    status_cv.notify_all();
}

QueryMPPCoordinator::~QueryMPPCoordinator()
{
    try
    {
        RuntimeFilterManager::getInstance().removeQuery(query_id);
        optimizer_context->getSegmentScheduler()->finishPlanSegments(query_id);
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("~QueryMPPCoordinator exception for query_id:{}", query_id));
    }
    QueryMPPManager::instance().clearQuery(query_id);
}

}
