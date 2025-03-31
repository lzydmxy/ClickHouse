#include "OptimizerContext.h"
#include <Core/Settings.h>
#include <Common/logger_useful.h>
#include <Coordination/Defines.h>
#include <Coordination/KeeperConstants.h>
#include <Server/CloudPlacementInfo.h>
#include <Coordination/KeeperFeatureFlags.h>
#include <Disks/DiskLocal.h>
#include <Disks/DiskSelector.h>
#include <IO/S3/Credentials.h>
#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Query/Executor/PlanSegmentInstance.h>

namespace DB
{

namespace ErrorCodes
{

extern const int BAD_ARGUMENTS;

}

class OptimizerContextData
{
public:
    OptimizerContextData(){}
    PlanSegmentInstanceID plan_segment_instance_id;
};

OptimizerContext::OptimizerContext(const Settings & settings_, OptimizerSettings & optimizer_settings_)
    :optimizer_settings(optimizer_settings_)
{
    if (settings_.max_execution_time.totalSeconds() != 0)
        query_max_execution_time = std::min(settings_.max_execution_time.totalSeconds() * UInt64(1000), UInt64(UINT32_MAX));
    else if (optimizer_settings.exchange_timeout_ms != 0)
        query_max_execution_time = std::min(UInt64(optimizer_settings.exchange_timeout_ms), UInt64(UINT32_MAX));
    else
        query_max_execution_time = 100 * 60 * 1000; // default as 100min
    data = std::make_shared<OptimizerContextData>();
}

UInt32 OptimizerContext::getQueryMaxExecutionTime() const
{
    return query_max_execution_time;
}

TimePoint OptimizerContext::getQueryExpirationTimeStamp() const
{
    return query_expiration_timestamp;
}

void OptimizerContext::initQueryExpirationTimeStamp()
{
    query_expiration_timestamp = std::chrono::system_clock::now() + std::chrono::milliseconds(query_max_execution_time);
}

void OptimizerContext::initExceptionHandler()
{
    exception_handler = std::make_shared<ExceptionHandler>();
}

ExceptionHandlerPtr OptimizerContext::getExceptionHandler() const
{
    return exception_handler;
}

void OptimizerContext::setCoordinatorAddress(const AddressInfoPtr address)
{
    coordinator_address = address;
}

AddressInfoPtr OptimizerContext::getCoordinatorAddress() const
{
    return coordinator_address;
}

void OptimizerContext::setRPCPort(UInt16 rpc_port_)
{
    rpc_port = rpc_port_;
}

UInt16 OptimizerContext::getRPCPort()
{
    return rpc_port;
}

void OptimizerContext::setPlanSegmentProcessListEntry(PlanSegmentProcessListEntryPtr segment_process_list_entry_)
{
    segment_process_list_entry = segment_process_list_entry_;
}

PlanSegmentProcessListEntryPtr OptimizerContext::getPlanSegmentProcessListEntry() const
{
    return segment_process_list_entry;
}

void OptimizerContext::setPlanSegmentProcessList(PlanSegmentProcessListPtr segment_process_list_)
{
    plan_segment_process_list = segment_process_list_;
}

PlanSegmentProcessListPtr OptimizerContext::getPlanSegmentProcessList() const
{
    return plan_segment_process_list;
}

void OptimizerContext::setProcessListEntry(ProcessListEntryPtr process_list_entry_)
{
    process_list_entry = process_list_entry_;
}

ProcessListEntryPtr OptimizerContext::getProcessListEntry() const
{
    return process_list_entry;
}

void OptimizerContext::setProcessListElement(QueryStatusPtr elem)
{
    query_process_element = elem;
}

QueryStatusPtr OptimizerContext::getProcessListElement() const
{
    return query_process_element;
}

void OptimizerContext::setSendTCPProgress(std::function<void()> callback)
{
    send_tcp_progress = callback;
}

std::function<void()> OptimizerContext::getSendTCPProgress() const
{
    return send_tcp_progress;
}

void OptimizerContext::setPlanSegmentInstanceID(const PlanSegmentInstanceID & instance_id)
{
    data->plan_segment_instance_id = instance_id;
}

PlanSegmentInstanceID OptimizerContext::getPlanSegmentInstanceID()
{
    return data->plan_segment_instance_id;
}

void OptimizerContext::setIsExplainQuery(const bool & is_explain_query_)
{
    is_explain_query = is_explain_query_;
}

bool OptimizerContext::isExplainQuery() const
{
    return is_explain_query;
}

void OptimizerContext::logOptimizerProfile(LoggerPtr log, String prefix, String name, UInt64 time, bool is_rule)
{
    if (optimizer_settings.log_optimizer_run_time && log)
        LOG_DEBUG(log, "{} {} {}", prefix, name, time);

//    if (optimizer_profile)
//        optimizer_profile->setTime(name, time, is_rule);
}

}
