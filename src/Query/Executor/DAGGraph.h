#pragma once
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <Common/logger_useful.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/Context.h>
#include <Query/Common/OptimizerContext.h>
#include <Query/ProtosHelper/AddressInfo.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/Executor/executePlanSegment.h>
#include <Query/Executor/PlanSegmentInstance.h>

#ifndef NDEBUG
#define TASK_ASSIGN_DEBUG
#endif

namespace DB
{
using SegmentIds = std::unordered_set<size_t>;
using WorkerInfoSet = std::unordered_set<HostWithPorts, std::hash<HostWithPorts>, HostWithPorts::IsSameEndpoint>;
using PlanSegmentId = size_t;
using StorageUnions = std::vector<std::unordered_set<UUID>>;
using StorageUnionsPtr = std::shared_ptr<StorageUnions>;
struct SourcePruner
{
    SourcePruner(PlanSegmentTree * plan_segments_ptr_, LoggerPtr log_)
        : plan_segments_ptr(plan_segments_ptr_), log(log_)
    {
    }
    
    void pruneSource(ContextPtr context, std::unordered_map<size_t, PlanSegment *> & id_to_segment);

    void prepare();
    
    std::unordered_set<PlanSegmentId> unprunable_plan_segments;
    std::unordered_map<PlanSegmentId, std::unordered_set<UUID>> plan_segment_storages_map;
    std::unordered_map<PlanSegmentId, WorkerInfoSet> plan_segment_workers_map;

private:
    void generateUnprunableSegments();
    void generateSegmentStorageMap();
    PlanSegmentTree * plan_segments_ptr;
    LoggerPtr log;
};

using SourcePrunerPtr = std::shared_ptr<SourcePruner>;

struct DAGGraph
{
    DAGGraph() : log(getLogger("DAGGraph")) 
    { 
        async_context = std::make_shared<AsyncContext>(); 
    }
    void joinAsyncRpcWithThrow();
    void joinAsyncRpcPerStage();
    void joinAsyncRpcAtLast();
    AddressInfos getAddressInfos(size_t segment_id);

    PlanSegment * getPlanSegmentPtr(size_t id);

    SourcePrunerPtr makeSourcePruner(PlanSegmentTree * plan_segments_ptr)
    {
        source_pruner = std::make_shared<SourcePruner>(plan_segments_ptr, log);
        return source_pruner;
    }

    void setContext(ContextPtr context)
    {
        query_context = context;
        optimizer_context = context->getOptimizerContext();
    }
    ContextPtr getContext()
    {
        return query_context;
    }

    /// all segments containing only table scan/value
    SegmentIds leaf_segments;
    /// all segments contain at least table scan/value
    SegmentIds table_scan_or_value_segments;
    size_t final = std::numeric_limits<size_t>::max();
    std::set<size_t> scheduled_segments;
    std::unordered_map<size_t, PlanSegment *> id_to_segment;
    std::unordered_map<size_t, std::pair<std::shared_ptr<PlanSegmentInput>, std::shared_ptr<PlanSegmentOutput>>> exchanges;
    std::unordered_map<size_t, AddressInfos> id_to_address;
    /// final worker address where task is successfully executed
    mutable std::mutex finished_address_mutex;
    std::unordered_map<size_t, std::map<size_t, AddressInfo>> finished_address;
    mutable bthread::Mutex status_mutex;
    std::set<AddressInfo> plan_send_addresses;
    PlanSegmentsStatusPtr plan_segment_status_ptr;
#if defined(TASK_ASSIGN_DEBUG)
    std::unordered_map<size_t, std::vector<std::pair<size_t, AddressInfo>>> exchange_data_assign_node_mappings;
#endif
    AsyncContextPtr async_context;
    std::unordered_map<size_t, UInt64> segment_parallel_size_map;
    butil::IOBuf query_common_buf;
    butil::IOBuf query_settings_buf;
    SourcePrunerPtr source_pruner;
private:
    LoggerPtr log;
    ContextPtr query_context = nullptr;
    OptimizerContextPtr optimizer_context = nullptr;
};

using DAGGraphPtr = std::shared_ptr<DAGGraph>;

} // namespace DB
