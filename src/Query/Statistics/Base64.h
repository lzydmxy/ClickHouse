#pragma once
#include <string_view>
#include <Core/Types.h>

namespace DB::QueryStatistics
{
    String base64Decode(std::string_view encoded);
    String base64Encode(std::string_view decoded);
}

namespace DB
{
    using QueryStatistics::base64Decode;
    using QueryStatistics::base64Encode;
}
