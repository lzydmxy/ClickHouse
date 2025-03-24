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
using PlanSegmentProcessListEntryWeakPtr = std::weak_ptr<PlanSegmentProcessListEntry>;
class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;
class HostWithPorts;

class SegmentScheduler;
using SegmentSchedulerPtr = std::shared_ptr<SegmentScheduler>;

class AddressInfo;
using AddressInfoPtr = std::shared_ptr<AddressInfo>;

class ProcessListEntry;
using ProcessListEntryPtr = std::shared_ptr<ProcessListEntry>;

struct ProcessorProfileLogElement;
template <typename>
class ProfileElementConsumer;

class QueryExchangeLog;
using QueryExchangeLogPtr = std::shared_ptr<QueryExchangeLog>;

struct Settings;
struct PlanSegmentInstanceID;

class OptimizerContextData;

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
    OptimizerContext(const Settings & settings_, OptimizerSettings & optimizer_settings_);

    const OptimizerSettings & getSettingsRef() const { return optimizer_settings; }
    const OptimizerSettings getSettings() const { return optimizer_settings; }

    /// milliseconds
    UInt32 getQueryMaxExecutionTime() const;
    TimePoint getQueryExpirationTimeStamp() const;
    void initQueryExpirationTimeStamp();

    void initExceptionHandler();
    ExceptionHandlerPtr getExceptionHandler() const;

    void setCoordinatorAddress(const AddressInfoPtr address);
    AddressInfoPtr getCoordinatorAddress() const;

    void setRPCPort(UInt16 rpc_port_);
    UInt16 getRPCPort();

    void setPlanSegmentProcessListEntry(PlanSegmentProcessListEntryPtr segment_process_list_entry_);
    PlanSegmentProcessListEntryPtr getPlanSegmentProcessListEntry() const;

    void setPlanSegmentProcessList(PlanSegmentProcessListPtr segment_process_list_);
    PlanSegmentProcessListPtr getPlanSegmentProcessList() const;

    void setProcessListEntry(ProcessListEntryPtr process_list_entry_);
    ProcessListEntryPtr getProcessListEntry() const;

    void
    setProcessorProfileElementConsumer(std::shared_ptr<ProfileElementConsumer<ProcessorProfileLogElement>> processor_log_element_consumer_);
    std::shared_ptr<ProfileElementConsumer<ProcessorProfileLogElement>> getProcessorProfileElementConsumer() const;

    void setProcessListElement(QueryStatusPtr elem);
    QueryStatusPtr getProcessListElement() const;

    void setPlanSegmentInstanceID(const PlanSegmentInstanceID & instance_id);
    PlanSegmentInstanceID getPlanSegmentInstanceID();

    void setSendTCPProgress(std::function<void()> callback);
    std::function<void()> getSendTCPProgress() const;

    HostWithPorts getHostWithPorts() const;
    SegmentSchedulerPtr getSegmentScheduler() const;

    ServiceType getServiceType() const;

    void setIsExplainQuery(const bool & is_explain_query_);
    bool isExplainQuery() const;

    QueryExchangeLogPtr getQueryExchangeLog();

private:
    OptimizerSettings optimizer_settings;
    UInt32 query_max_execution_time;
    TimePoint query_expiration_timestamp;
    AddressInfoPtr coordinator_address;
    UInt16 rpc_port;
    std::shared_ptr<OptimizerContextData> data;
    ExceptionHandlerPtr exception_handler;
    PlanSegmentProcessListEntryPtr segment_process_list_entry;
    PlanSegmentProcessListPtr plan_segment_process_list;
    ProcessListEntryPtr process_list_entry;
    QueryStatusPtr query_process_element;
    std::function<void()> send_tcp_progress{nullptr};
    bool is_explain_query{false};
    QueryExchangeLogPtr query_exchange_log;
};

using OptimizerContextPtr = std::shared_ptr<OptimizerContext>;

}
