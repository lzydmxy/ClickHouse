#pragma once
#include <mutex>
#include <string_view>
#include <condition_variable>
#include <Poco/Logger.h>
#include <base/types.h>
#include <Interpreters/Context_fwd.h>
#include <QueryPipeline/BlockIO.h>
#include <Query/Common/OptimizerContext.h>
#include <Query/Executor/ExecutorUtils.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/Executor/ProgressManager.h>
#include <Query/Executor/RuntimeSegmentsStatus.h>

namespace DB
{
class PlanSegmentTree;
struct BlockIO;

enum PostProcessingRPCID : uint8_t
{
    ReportPlanSegmentCost = 0
};

struct QueryMPPOptions
{
    bool need_all_instance_result{false};
};

/** Execute the query through MPP coordinator
  */
class QueryMPPCoordinator final: public std::enable_shared_from_this<QueryMPPCoordinator>
{
public:
    QueryMPPCoordinator(PlanSegmentTreeUniqPtr plan_segment_tree_, ContextMutablePtr query_context_, QueryMPPOptions options_);
    /// Invoke this in InterpreterSelectQueryUseOptimizer's execute method
    BlockIO execute();

    SummarizedQueryStatus waitUntilFinish(int error_code, const String & error_msg);

    //TODO: redefine RuntimeSegmentsStatus
    void updateSegmentInstanceStatus(const RuntimeSegmentStatus & status);

    /// normal progress received from sendProgress rpc
    void onProgress(UInt32 segment_id, UInt32 parallel_index, const Progress & progress_);
    /// final progress received from updatePlanSegmentStatus
    void onFinalProgress(UInt32 segment_id, UInt32 parallel_index, const Progress & progress_);
    /// final progress is the last progress received from worker instance, and is assumed to contain all past progress
    Progress getFinalProgress() const;

    /// initialize post_processing_rpc_waiting, including all plan segments except the final plan segment.
    void initializePostProcessingRPCReceived();
    /// wait unitl all post processing rpcs have been received.
    void waitUntilAllPostProcessingRPCReceived();

    void tryUpdateRootErrorCause(const QueryError & query_error, bool is_canceled);

    ContextPtr getContext() { return query_context; }

    ~QueryMPPCoordinator();

    UInt64 getNormalizedQueryPlanHash() const
    {
        return normalized_query_plan_hash;
    }

private:
    // template <class Event>
    // boost::msm::back::HandledEnum triggerEvent(Event const & evt); // It use state_machine_mutex;

    ContextMutablePtr query_context;
    OptimizerContextPtr optimizer_context;
    QueryMPPOptions options;
    PlanSegmentTreePtr plan_segment_tree;
    const String & query_id;
    ProgressManager progress_manager;
    LoggerPtr log;

    mutable std::mutex status_mutex;
    std::condition_variable status_cv;
    QueryMPPStatus query_status;

    mutable std::mutex post_processing_rpc_waiting_mutex;
    std::condition_variable post_processing_rpc_waiting_cv;
    std::unordered_map<PostProcessingRPCID, PlanSegmentSet> post_processing_rpc_waiting = {};
    bool post_processing_rpc_waiting_initialized = false;

    UInt64 normalized_query_plan_hash = 0;
};

using QueryMPPCoordinatorPtr = std::shared_ptr<QueryMPPCoordinator>;
using CoordinatorWeakPtr = std::weak_ptr<QueryMPPCoordinator>;
using CoordinatorMap = std::unordered_map<String, CoordinatorWeakPtr>;

}
