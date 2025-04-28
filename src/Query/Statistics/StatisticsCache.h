#pragma once

#include <Query/Statistics/StatsTableIdentifier.h>
#include <Query/Statistics/StatisticsBase.h>
#include <Query/Statistics/StatisticsCollectorObjects.h>
#include <Query/Statistics/StatsTableIdentifier.h>

#include <chrono>
#include <shared_mutex>

namespace DB::QueryStatistics
{
    namespace chrono = std::chrono;

    class StatisticsCache
    {
    public:
        explicit StatisticsCache(const chrono::nanoseconds & expire_time_) : expire_time(expire_time_) { }

        struct CacheEntry
        {
            // use shared ptr, nullptr
            std::shared_ptr<StatsData> data;
            chrono::time_point<chrono::steady_clock, chrono::nanoseconds> expire_time_point;
        };

        void invalidate(const UUID & table);
        std::shared_ptr<StatsData> get(const UUID & table);
        void update(const UUID & table, std::shared_ptr<StatsData> data);
        void clear();

    private:
        chrono::nanoseconds expire_time;
        std::unordered_map<UUID, CacheEntry> impl;
        std::shared_mutex mutex;
    };
}
