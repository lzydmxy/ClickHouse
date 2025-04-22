#pragma once

#include <Query/Statistics/StatisticsBase.h>
#include <Common/Exception.h>
#include <Query/Statistics/SerdeUtils.h>

namespace DB::QueryStatistics
{
    template <class StatsDerived>
    inline void checkTag(StatisticsTag tag)
    {
        if (StatsDerived::tag != tag)
        {
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Statistics Tag mismatch");
        }
    }

    template <class StatsType>
    std::shared_ptr<StatsType> createStatisticsTyped(StatisticsTag tag, std::string_view blob);

    template <class StatsType>
    std::shared_ptr<StatsType> createStatisticsUntyped(StatisticsTag tag, std::string_view blob);

}
