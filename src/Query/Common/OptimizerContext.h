#pragma once
#include <Poco/Util/AbstractConfiguration.h>
#include <Query/Common/QueryCommon.h>
#include <Query/Common/ExceptionHandler.h>
#include <Query/Common/OptimizerSettings.h>

namespace DB
{

class ExceptionHandler;
using ExceptionHandlerPtr = std::shared_ptr<ExceptionHandler>;
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

class ProcessListEntry;
using ProcessListEntryPtr = std::shared_ptr<ProcessListEntry>;

class QueryExchangeLog;
using QueryExchangeLogPtr = std::shared_ptr<QueryExchangeLog>;

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
    OptimizerContext(OptimizerSettingsPtr query_settings_);

    const OptimizerSettingsPtr & getSettings() const;
    const OptimizerSettings & getSettingsRef() const { return settings; }

    void initExceptionHandler();
    ExceptionHandlerPtr getExceptionHandler() const;

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
    TimePoint getQueryExpirationTimeStamp() const;
    void initQueryExpirationTimeStamp();

    void setProcessListEntry(ProcessListEntryPtr process_list_entry_);
    ProcessListEntryPtr getProcessListEntry() const;

    void setSendTCPProgress(std::function<void()> callback);
    std::function<void()> getSendTCPProgress() const;

    void setIsExplainQuery(const bool & is_explain_query_);
    bool isExplainQuery() const;

    QueryExchangeLogPtr getQueryExchangeLog();

private:
    OptimizerSettingsPtr query_settings;
    OptimizerSettings settings;
    AddressInfoPtr coordinator_address;
    ExceptionHandlerPtr exception_handler;
    PlanSegmentProcessListPtr plan_segment_process_list;
    ProcessListEntryPtr process_list_entry;
    std::function<void()> send_tcp_progress{nullptr};
    bool is_explain_query{false};
    QueryExchangeLogPtr query_exchange_log;
};

using OptimizerContextPtr = std::shared_ptr<OptimizerContext>;

}
