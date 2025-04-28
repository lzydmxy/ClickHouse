#pragma once
#include <Core/Types.h>
#include <string_view>

namespace DB::QueryStatistics
{
    UInt64 stringHash64(std::string_view view);
}
