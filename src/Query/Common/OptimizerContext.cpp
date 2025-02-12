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
#include <Query/Executor/PlanSegmentInstance.h>

namespace DB
{

namespace ErrorCodes
{

extern const int BAD_ARGUMENTS;

}

OptimizerContext::OptimizerContext(QuerySettingsPtr query_settings_)
    : query_settings(std::move(query_settings_))
{
}

PlanSegmentProcessListPtr OptimizerContext::getPlanSegmentProcessList()
{
    return plan_segment_process_list;
}

// void OptimizerContext::setPlanSegmentInstanceID(const PlanSegmentInstanceID & instance_id)
// {

// }

// PlanSegmentInstanceID OptimizerContext::getPlanSegmentInstanceID() const
// {

// }

}
