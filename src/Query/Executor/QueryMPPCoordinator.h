#pragma once
#include <Common/Logger.h>
#include <memory>
#include <mutex>
#include <string_view>
#include <Interpreters/Context_fwd.h>
#include <QueryPipeline/BlockIO.h>
#include <Query/Executor/ExecutorUtils.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/Interpreters/RuntimeSegmentsStatus.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <Poco/Logger.h>
#include <common/types.h>

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
    ContextMutablePtr query_context;
    QueryMPPOptions options;
    PlanSegmentTreePtr plan_segment_tree;
    const String & query_id;
    ProgressManager progress_manager;
    LoggerPtr log;

    mutable bthread::Mutex status_mutex;
    bthread::ConditionVariable status_cv;
    MPPQueryStatus query_status;

    mutable bthread::Mutex post_processing_rpc_waiting_mutex;
    bthread::ConditionVariable post_processing_rpc_waiting_cv;
    std::unordered_map<PostProcessingRPCID, PlanSegmentSet> post_processing_rpc_waiting = {};
    bool post_processing_rpc_waiting_initialized = false;

    UInt64 normalized_query_plan_hash = 0;
};

using QueryMPPCoordinatorPtr = std::shared_ptr<QueryMPPCoordinator>;

}
