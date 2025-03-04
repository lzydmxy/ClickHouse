#include "OptimizerContext.h"

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
// #include <Query/Executor/PlanSegmentInstance.h>

namespace DB
{

namespace ErrorCodes
{

extern const int BAD_ARGUMENTS;

}

OptimizerContext::OptimizerContext(OptimizerSettingsPtr query_settings_)
    : query_settings(std::move(query_settings_))
{
}

AddressInfoPtr OptimizerContext::getCoordinatorAddress() const
{
    return coordinator_address;
}

void OptimizerContext::setCoordinatorAddress(AddressInfoPtr address)
{
    coordinator_address = address;
}

void OptimizerContext::initExceptionHandler()
{
    exception_handler = std::make_shared<ExceptionHandler>();
}

ExceptionHandlerPtr OptimizerContext::getExceptionHandler() const
{
    return exception_handler;
}

PlanSegmentProcessListPtr OptimizerContext::getPlanSegmentProcessList()
{
    return plan_segment_process_list;
}

void OptimizerContext::setProcessListEntry(std::shared_ptr<ProcessListEntry> process_list_entry_)
{
    process_list_entry = process_list_entry_;
}

std::shared_ptr<ProcessListEntry> OptimizerContext::getProcessListEntry() const
{
    return process_list_entry;
}

void OptimizerContext::setSendTCPProgress(std::function<void()> callback)
{
    send_tcp_progress = callback;
}

std::function<void()> OptimizerContext::getSendTCPProgress() const
{
    return send_tcp_progress;
}

// void OptimizerContext::setPlanSegmentInstanceID(const PlanSegmentInstanceID & instance_id)
// {

// }

// PlanSegmentInstanceID OptimizerContext::getPlanSegmentInstanceID() const
// {

// }

void OptimizerContext::setIsExplainQuery(const bool & is_explain_query_)
{
    is_explain_query = is_explain_query_;
}

bool OptimizerContext::isExplainQuery() const
{
    return is_explain_query;
}

}
