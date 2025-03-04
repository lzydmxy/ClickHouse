#pragma once
#include <base/types.h>
#include <brpc/server.h>
#include <Common/logger_useful.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <Query/ProtosHelper/QueryProto.h>
#include <Query/Common/ResourceMonitor.h>
#include <Query/Executor/QueryMPPCoordinator.h>
#include <Query/Executor/QueryMPPManager.h>
#include <Query/Executor/PlanSegmentProcessList.h>
#include <Query/Exchange/bRPC/BrpcServiceDefines.h>


namespace DB
{
//class SegmentScheduler;
class Context;

using SettingsChangesPtr = std::shared_ptr<SettingsChanges>;

class RepeatedTimerTask {
public:
    RepeatedTimerTask(BackgroundSchedulePool &pool_, UInt64 interval_, const std::string& name_);

    virtual ~RepeatedTimerTask() = default;

    void start()
    {
        task->activateAndSchedule();
    }

    void stop()
    {
        task->deactivate();
    }

protected:
    virtual void run() = 0;
    UInt64 interval; /// in seconds;
    BackgroundSchedulePool::TaskHolder task;
};

class ResourceMonitorTimer : public RepeatedTimerTask {
public:
    ResourceMonitorTimer(ContextMutablePtr & global_context_, UInt64 interval_, const std::string& name_, LoggerPtr log_) :
        RepeatedTimerTask(global_context_->getSchedulePool(), interval_, name_), resource_monitor(global_context_) {
        log = log_;
    }
    virtual ~ResourceMonitorTimer() override {}
    virtual void run() override;
    //WorkerNodeResourceData getResourceData() const;
    void updateResourceData();

private:
    ResourceMonitor resource_monitor;
    //WorkerNodeResourceData cached_resource_data;
    mutable std::mutex resource_data_mutex;
    LoggerPtr log;
};

using ResourceMonitorTimerPtr = std::unique_ptr<ResourceMonitorTimer>;

class PlanSegmentRpcService : public RPlanSegmentService
{
public:
    explicit PlanSegmentRpcService(ContextMutablePtr context_)
        : context(context_)
        , log(getLogger("PlanSegmentRpcService"))
    {
        report_metrics_timer = std::make_unique<ResourceMonitorTimer>(context, 1000, "ResourceMonitorTimer", log);
        report_metrics_timer->start();
    }

    ~PlanSegmentRpcService() override
    {
        try
        {
            LOG_DEBUG(log, "Waiting report metrics timer finishing");
            report_metrics_timer->stop();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    /// execute query described by plan segment
    void executePlanSegment(
        ::google::protobuf::RpcController * controller,
        const RPlanSegmentRequest * request,
        RPlanSegmentResponse * response,
        ::google::protobuf::Closure * done) override;

    /// execute queries described by plan segments
    void executePlanSegments(
        ::google::protobuf::RpcController * controller,
        const RPlanSegmentsRequest * request,
        RPlanSegmentResponse * response,
        ::google::protobuf::Closure * done) override;

    /// receive exception report send terminate query (coordinate host ---> segment executor host)
    void cancelQuery(
        ::google::protobuf::RpcController * /*controller*/,
        const RCancelQueryRequest * request,
        RCancelQueryResponse * response,
        ::google::protobuf::Closure * done) override;

    /// report plan segment status (segment executor host --> coordinator host)
    void reportPlanSegmentStatus(
        ::google::protobuf::RpcController * /*controller*/,
        const RPlanSegmentStatusRequest * request,
        RPlanSegmentStatusResponse * /*response*/,
        ::google::protobuf::Closure * done) override;

    void reportPlanSegmentProfile(
        ::google::protobuf::RpcController * /*controller*/,
        const RPlanSegmentProfileRequest * request,
        RPlanSegmentProfileResponse * /*response*/,
        ::google::protobuf::Closure * done) override;
private:
    void prepareCommonParams(
        UInt32 major_revision,
        UInt32 query_common_buf_size,
        UInt32 query_settings_buf_size,
        brpc::Controller * cntl,
        RQueryCommonPtr & query_common,
        SettingsChangesPtr & settings_changes);

    // can be call both sync or async
    static ContextMutablePtr createQueryContext(
        ContextMutablePtr global_context,
        RQueryCommonPtr & query_common,
        UInt16 remote_side_port,
        PlanSegmentInstanceID instance_id);

    // only can be call in async thread
    static void initQueryContext(
        ContextMutablePtr query_context,
        RQueryCommonPtr query_common,
        SettingsChangesPtr settings_changes,
        const AddressInfo & execution_address);

    void innerExecutePlanSegment(
        RQueryCommonPtr query_common,
        SettingsChangesPtr settings_changes,
        UInt16 remote_side_port,
        UInt32 segment_id,
        PlanSegmentExecutionInfo & execution_info,
        std::shared_ptr<butil::IOBuf> plan_segment_buf,
        PlanSegmentProcessList::EntryPtr process_plan_segment_entry = nullptr,
        ContextMutablePtr query_context = nullptr);

    ContextMutablePtr context;
    ResourceMonitorTimerPtr report_metrics_timer;
    LoggerPtr log;
};

REGISTER_SERVICE_IMPL(PlanSegmentRpcService);

}
