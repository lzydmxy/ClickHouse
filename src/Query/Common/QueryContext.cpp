#include "QueryContext.h"

#include <atomic>
#include <chrono>

#include <Coordination/Defines.h>
#include <Coordination/KeeperConstants.h>
#include <Server/CloudPlacementInfo.h>
#include <Coordination/KeeperFeatureFlags.h>
#include <Disks/DiskLocal.h>
#include <Disks/DiskSelector.h>
#include <IO/S3/Credentials.h>
#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/logger_useful.h>

#include <boost/algorithm/string.hpp>

namespace DB
{

namespace ErrorCodes
{

extern const int BAD_ARGUMENTS;

}

QueryContext::QueryContext(QuerySettingsPtr query_settings_)
    : query_settings(std::move(query_settings_))
{
}

}
