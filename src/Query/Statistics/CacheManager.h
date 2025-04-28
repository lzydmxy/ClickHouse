#pragma once

#include <DataTypes/DataTypeUUID.h>
#include <Interpreters/Context.h>
#include <Query/Statistics/StatisticsBase.h>
#include <Query/Statistics/StatisticsCache.h>
#include <Query/Statistics/StatsTableIdentifier.h>

namespace DB::QueryStatistics
{

    class CacheManager
    {
    public:
        struct KeyHash
        {
            auto operator()(const std::pair<UUID, String> & key) const
            {
                return std::hash<UUID>()(key.first) ^ std::hash<String>()(key.second);
            }
        };
        using CacheType = StatisticsCache;

        static void initialize(ContextPtr context);
        // for testing
        static void initialize(UInt64 entry_size, std::chrono::seconds expire_time);

        static CacheType & instance()
        {
            if (!cache)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "cache has to be initialized");
            }
            return *cache;
        }

        // invalidate cache on current server
        static void invalidate(const ContextPtr context, const StatsTableIdentifier & table);
        static void reset();

    private:
        static std::unique_ptr<CacheType> cache;
    };

}
