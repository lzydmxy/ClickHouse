#pragma once
#include <Query/Statistics/Histogram.h>

namespace DB::QueryStatistics
{
    Histogram simplifyHistogram(const Histogram & origin, double ndv_density_threshold, double range_density_threshold, bool is_integer);
}
