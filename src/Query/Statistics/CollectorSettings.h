#pragma once

#include <Core/Settings.h>
#include <Query/Statistics/SettingsMap.h>

namespace DB::QueryStatistics
{
    // don't change this name since it is used everywhere
    using CollectorSettings = QueryStatistics::CreateStatsSettings;
}
