#pragma once
#include <brpc/controller.h>
#include <Interpreters/Context_fwd.h>
#include <QueryPipeline/BlockIO.h>
#include <Query/Common/WorkerID.h>
#include <Query/ProtosHelper/AddressInfo.h>
#include <Query/ProtosHelper/QueryProto.h>
#include <Query/ProtosHelper/RPCHelpers.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/Executor/PlanSegmentInstance.h>
#include <Query/Executor/PlanSegmentProcessList.h>

namespace butil
{
class IObuf;
}

namespace std
{
template <>
struct hash<brpc::CallId>
{
    std::size_t operator()(const brpc::CallId & id) const { return hash<uint64_t>{}(id.value); }
};
}

namespace DB
{
class Context;

struct AsyncContext
{
    enum AsyncStats : uint8_t
    {
        INIT = 0,
        SUCCESS = 1,
        FAILED = 2
    };

    struct AsyncResult
    {
        bool is_success{true};
        std::string error_text;
        std::string failed_worker;
        AsyncStats status{AsyncStats::INIT};
        int error_code{0};
    };

    void asyncComplete(brpc::CallId id, AsyncResult & async_result);
    void addCallId(brpc::CallId id);
    AsyncResult wait();

    std::unordered_set<brpc::CallId> call_ids;
    std::mutex mutex;
    std::condition_variable cv;
    AsyncResult result;
};

using AsyncContextPtr = std::shared_ptr<AsyncContext>;

struct PlanSegmentHeader
{
    PlanSegmentInstanceID instance_id;
    size_t plan_segment_buf_size = 0;
    IOBufPtr plan_segment_buf_ptr;
    UInt32 attempt_id = std::numeric_limits<UInt32>::max();
    SourceTaskFilter source_task_filter{};
    void toProto(RPlanSegmentHeader & proto) const
    {
        proto.set_plan_segment_id(instance_id.segment_id);
        proto.set_parallel_id(instance_id.parallel_index);
        proto.set_plan_segment_buf_size(plan_segment_buf_size);
        proto.set_attempt_id(attempt_id);
        if (source_task_filter.isValid())
            *proto.mutable_source_task_filter() = source_task_filter.toProto();
    }
};

// Currently worker_id is included in address_info, but will need to be separated in the future
struct AddressWithWorkerID
{
    AddressInfo address_info;
    WorkerID worker_id;
    inline bool operator==(AddressWithWorkerID const & rhs) const
    {
        return (this->address_info == rhs.address_info && this->worker_id == rhs.worker_id);
    }
    class Hash
    {
    public:
        size_t operator()(const AddressWithWorkerID & key) const
        {
            return AddressInfo::Hash()(key.address_info);
        }
    };
};

using PlanSegmentHeaders = std::vector<PlanSegmentHeader>;
using BatchPlanSegmentHeaders = std::unordered_map<AddressWithWorkerID, PlanSegmentHeaders, AddressWithWorkerID::Hash>;

/// Local execute plan segment in coodinator
BlockIO executePlanSegmentClient(PlanSegmentInstancePtr plan_segment_instance, ContextMutablePtr context);

void executePlanSegmentInternal(
    PlanSegmentInstancePtr plan_segment_instance,
    ContextMutablePtr context,
    PlanSegmentProcessList::EntryPtr process_plan_segment_entry,
    bool async);

void prepareQueryCommonBuf(butil::IOBuf & common_buf, const PlanSegment & any_plan_segment, ContextPtr & context);

void executePlanSegmentRemotelyWithPreparedBuf(
    size_t segment_id,
    PlanSegmentExecutionInfo execution_info,
    const butil::IOBuf & query_common_buf,
    const butil::IOBuf & query_settings_buf,
    const butil::IOBuf & plan_segment_buf,
    AsyncContextPtr & async_context,
    const Context & context,
    const WorkerID & worker_id = WorkerID{});

void executePlanSegmentsRemotely(
    const AddressInfo & address_info,
    const PlanSegmentHeaders & plan_segment_headers,
    const butil::IOBuf & query_common_buf,
    const butil::IOBuf & query_settings_buf,
    AsyncContextPtr & async_context,
    const Context & context,
    const WorkerID & worker_id = WorkerID{});
}
