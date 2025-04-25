#pragma once

#include <string>
#include <vector>

namespace DB::QueryStatistics
{
    template <typename T>
    std::vector<T> vectorDeserialize(std::string_view blob);

    template <typename T>
    std::string vectorSerialize(const std::vector<T> & data);
}
