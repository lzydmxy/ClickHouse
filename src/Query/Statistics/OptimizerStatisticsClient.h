#pragma once

#include <Query/Statistics/StatisticsSettings.h>

namespace DB::QueryStatistics
{
    void refreshClusterStatsCache(ContextPtr context, const StatsTableIdentifier & table_identifier, bool is_drop);
    StatisticsSettings fetchStatisticsSettings(ContextPtr context);
}
