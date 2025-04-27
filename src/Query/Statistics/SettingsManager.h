#pragma once

#include <Query/Statistics/SettingsMap.h>
#include <Query/Statistics/StatisticsBase.h>
#include <Query/Statistics/StatisticsBaseImpl.h>
#include <Query/Statistics/StatisticsSettings.h>
#include <Query/Statistics/StatsTableIdentifier.h>
#include <Query/Statistics/TypeUtils.h>
#include <Common/SettingsChanges.h>
#include <Interpreters/Context_fwd.h>

#include <fmt/format.h>

#include <map>
#include <shared_mutex>

namespace DB::QueryStatistics
{

    // In Memory Settings and its zookeeper mirror
    class SettingsManager: WithContext
    {
    public:
        using TableSettings = StatisticsSettings::TableSettings;
        // todo: bc, change it to catalog
        explicit SettingsManager(ContextPtr context_): WithContext(context_)
        {
        }

        // void enableAutoStatsTasks(const StatisticsScope & scope, SettingsChanges settings_changes);
        // void disableAutoStatsTasks(const StatisticsScope & scope);
        // void alterManagerSettings(const SettingsChanges & settings);

        TableSettings getTableSettings(const StatsTableIdentifier & identifier);

        AutoStatsManagerSettings getManagerSettings();

        // only for show auto_stats command, always copy all to avoid data race
        auto getFullStatisticsSettings()
        {
            std::shared_lock lck(mutex);
            return data;
        }

        void loadSettingsFromXml(const Poco::Util::AbstractConfiguration & config);

    private:

        StatisticsSettings data;
        mutable std::shared_mutex mutex;
    };
}
