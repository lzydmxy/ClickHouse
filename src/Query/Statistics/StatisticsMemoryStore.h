#pragma once

#include <Query/Statistics/StatisticsBase.h>
#include <Query/Statistics/StatsTableIdentifier.h>
#include <boost/noncopyable.hpp>

#include <shared_mutex>

namespace DB::QueryStatistics
{

    struct TableEntry
    {
        StatsTableIdentifier identifier;
        StatsData data;
    };

    using TableEntryPtr = std::shared_ptr<TableEntry>;

    struct StatisticsMemoryStore : boost::noncopyable
    {
        using UniqueKey = StatsTableIdentifier::UniqueKey;
        std::shared_mutex mtx;
        std::unordered_map<UniqueKey, std::shared_ptr<TableEntry>> entries;
    };

}
