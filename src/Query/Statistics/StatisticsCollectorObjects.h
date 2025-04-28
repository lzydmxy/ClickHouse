#pragma once

#include <Query/Statistics/StatsColumnBasic.h>
#include <Query/Statistics/StatsHllSketch.h>
#include <Query/Statistics/StatsKllSketch.h>
#include <Query/Statistics/StatsNdvBuckets.h>
#include <Query/Statistics/StatsTableBasic.h>

namespace DB::QueryStatistics
{
namespace StatisticsImpl
{
    struct TableStats
    {
    public:
        std::shared_ptr<StatsTableBasic> basic;

        // todo: bc, use reflection to eliminate this manual code
        StatsCollection writeToCollection() const
        {
            std::unordered_map<StatisticsTag, StatisticsBasePtr> collection;
            if (basic)
                collection.emplace(basic->getTag(), basic);
            return collection;
        }

        // todo: bc, use reflection to eliminate this manual code
        void readFromCollection(const StatsCollection & collection)
        {
            auto tag = StatisticsTag::TableBasic;
            if (collection.count(tag))
                basic = std::static_pointer_cast<StatsTableBasic>(collection.at(tag));
        }
    };

    struct ColumnStats
    {
    public:
        std::shared_ptr<StatsNdvBucketsResult> ndv_buckets_result;
        // basic contains ndv and histogram bounds, a.k.a. hll/kll sketch
        std::shared_ptr<StatsColumnBasic> basic;


        // todo: bc, use reflection to eliminate this manual code
        StatsCollection writeToCollection() const
        {
            std::unordered_map<StatisticsTag, StatisticsBasePtr> collection;

            auto list = std::vector<StatisticsBasePtr>{ndv_buckets_result, basic};

            for (auto & ptr : list)
            {
                if (ptr)
                    collection.emplace(ptr->getTag(), ptr);
            }
            return collection;
        }

        // todo: bc, use reflection to eliminate this manual code
        void readFromCollection(const StatsCollection & collection)
        {
            auto handle = [&]<typename T>(std::shared_ptr<T> & field) {
                using StatsType = T;
                constexpr auto tag = StatsType::tag;
                if (collection.count(tag))
                {
                    field = std::static_pointer_cast<StatsType>(collection.at(tag));
                }
            };

            handle(ndv_buckets_result);
            handle(basic);
        }
    };

    using ColumnStatsMap = std::map<String, ColumnStats>;
} // namespace StatisticsImpl
}
