#pragma once
#include <Poco/Util/AbstractConfiguration.h>
#include <Query/Common/ExceptionHandler.h>

namespace DB
{

struct QuerySettings;
using QuerySettingsPtr = std::shared_ptr<QuerySettings>;
class PlanSegmentProcessList;
using PlanSegmentProcessListPtr = std::shared_ptr<PlanSegmentProcessList>;
class PlanSegmentProcessListEntry;
using PlanSegmentProcessListEntryPtr = std::shared_ptr<PlanSegmentProcessListEntry>;
class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;
class HostWithPorts;

class SegmentScheduler;
using SegmentSchedulerPtr = std::shared_ptr<SegmentScheduler>;

class AddressInfo;
using AddressInfoPtr = std::shared_ptr<AddressInfo>;

struct PlanSegmentInstanceID;

enum ServiceType
{
    standalone,
    node,
    master,
    tso
};

class OptimizerContext
{
public:
    OptimizerContext(QuerySettingsPtr query_settings_);

    const QuerySettingsPtr & getQuerySettings() const;

    void initPlanSegmentExHandler();
    ExceptionHandlerPtr getPlanSegmentExHandler() const;

    void setPlanSegmentProcessListEntry(PlanSegmentProcessListEntryPtr segment_process_list_entry_);
    PlanSegmentProcessListPtr getPlanSegmentProcessList();

    void setProcessListElement(QueryStatusPtr elem);

    HostWithPorts getHostWithPorts() const;

    SegmentSchedulerPtr getSegmentScheduler() const;

    ServiceType getServiceType() const;

    void setCoordinatorAddress(const AddressInfoPtr address);
    AddressInfoPtr getCoordinatorAddress() const;

    void setPlanSegmentInstanceID(const PlanSegmentInstanceID & instance_id);
    PlanSegmentInstanceID getPlanSegmentInstanceID() const;

    UInt32 getQueryMaxExecutionTime() const;
    timespec getQueryExpirationTimeStamp() const;
    void initQueryExpirationTimeStamp();
private:
    QuerySettingsPtr query_settings;
    PlanSegmentProcessListPtr plan_segment_process_list;
};

using OptimizerContextPtr = std::shared_ptr<OptimizerContext>;

}
