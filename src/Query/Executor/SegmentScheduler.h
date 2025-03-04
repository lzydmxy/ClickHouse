#pragma once
#include <unordered_map>
#include <unordered_set>
#include <brpc/controller.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/CancellationCode.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Parsers/IAST_fwd.h>
#include <Query/ProtosHelper/QueryProto.h>
#include <Query/ProtosHelper/AddressInfo.h>
#include <Query/Common/ConcurrentShardMap.h>
#include <Query/Executor/DAGGraph.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/Executor/PlanSegmentExecutor.h>
#include <Query/Executor/Scheduler.h>


namespace DB
{

struct ExceptionWithCode
{
    ExceptionWithCode(const String & exception_, int code_) : exception(exception_), code(code_) { }
    String exception;
    int code;
};

using RuntimeSegmentsStatusPtr = std::shared_ptr<RuntimeSegmentStatus>;
struct PlanSegmentProfile;
using PlanSegmentProfilePtr = std::shared_ptr<PlanSegmentProfile>;
using PlanSegmentProfiles = std::vector<PlanSegmentProfilePtr>;
using PlanSegmentsStatusPtr = std::shared_ptr<PlanSegmentsStatus>;
using PlanSegmentsPtr = std::vector<PlanSegmentPtr>;
// <query_id, <segment_id, set of segment's received status for each instance >>
using RuntimeSegmentsStatusCounter = std::unordered_map<size_t, std::unordered_set<UInt64>>;
// <query_id, <segment_id, status>>
using SegmentStatusMap = std::unordered_map<String, std::unordered_map<size_t, RuntimeSegmentsStatusPtr>>;
using SegmentProfilesMap = std::unordered_map<String, std::unordered_map<size_t, PlanSegmentProfiles>>;

enum class OverflowMode;

struct SegmentSchedulerOptions
{
    std::function<void()> send_progress_callback;
};

class SegmentScheduler
{
public:
    SegmentScheduler(): log(getLogger("SegmentScheduler")) {}
    virtual ~SegmentScheduler() {}
    PlanSegmentsStatusPtr insertPlanSegments(const String & query_id,
                                             PlanSegmentTree * plan_segments_ptr,
                                             ContextPtr query_context);

    CancellationCode
    cancelPlanSegmentsFromCoordinator(const String & query_id, const Int32 & code, const String & exception, ContextPtr query_context);
    CancellationCode cancelPlanSegments(
        const String & query_id,
        const Int32 & code,
        const String & exception,
        const String & origin_host_name,
        ContextPtr query_context,
        std::shared_ptr<DAGGraph> dag_graph_ptr = nullptr);

    void cancelWorkerPlanSegments(const String & query_id, DAGGraphPtr dag_ptr, ContextPtr query_context);

    bool finishPlanSegments(const String & query_id);

    AddressInfos getWorkerAddress(const String & query_id, size_t segment_id);

    void checkQueryCpuTime(const String & query_id);
    void updateSegmentStatus(const RuntimeSegmentStatus & segment_status);
    void updateQueryStatus(const RuntimeSegmentStatus & segment_status);

    void updateSegmentProfile(PlanSegmentProfilePtr & segment_profile);
    std::unordered_map<size_t, PlanSegmentProfiles> getSegmentsProfile(const String & query_id);

    void updateReceivedSegmentStatusCounter(const String & query_id, const size_t & segment_id, const UInt64 & parallel_index);
    // Return true if only the query runs in bsp mode and all statuses of specified segment has been received.
    bool bspQueryReceivedAllStatusOfSegment(const String & query_id, const size_t & segment_id) const;
    bool alreadyReceivedAllSegmentStatus(const String & query_id);
    void onSegmentFinished(const RuntimeSegmentStatus & status);

    PlanSegmentSet getIOPlanSegmentInstanceIDs(const String & query_id) const;

    void workerRestarted(const WorkerID & id, const HostWithPorts & host_ports, UInt32 register_time);

private:
    // Protect `query_map`.
    mutable bthread::Mutex mutex;
    std::unordered_map<String, std::shared_ptr<DAGGraph>> query_map;

    // Protect maps below.
    mutable bthread::Mutex segment_status_mutex;
    mutable bthread::Mutex segment_profile_mutex;
    mutable SegmentStatusMap segment_status_map;
    mutable SegmentProfilesMap segment_profile_map;
    mutable std::unordered_map<String, RuntimeSegmentsStatusPtr> query_status_map;
    // record exception when exception occurred
    ConcurrentShardMap<String, ExceptionWithCode> query_to_exception_with_code;
    std::unordered_map<String, RuntimeSegmentsStatusCounter> query_status_received_counter_map;

    LoggerPtr log;

    void buildDAGGraph(PlanSegmentTree * plan_segments_ptr, std::shared_ptr<DAGGraph> graph);
    PlanSegmentExecutionInfo scheduleV2(const String & query_id, ContextPtr query_context, std::shared_ptr<DAGGraph> dag_graph_ptr);

protected:
};

using SegmentSchedulerPtr = std::shared_ptr<SegmentScheduler>;

}
