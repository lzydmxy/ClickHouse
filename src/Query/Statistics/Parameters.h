#pragma once

#include <Core/Types.h>

namespace DB::QueryStatistics
{
    namespace ConfigParameters
    {
        constexpr UInt64 max_cache_size = 1024UL * 1024UL;
        constexpr UInt64 cache_expire_time = 1800;
    }
}
