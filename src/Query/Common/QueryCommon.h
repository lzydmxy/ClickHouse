#pragma once
#include <set>
#include <string>

namespace DB
{

using PlanNodeId = UInt32;

template <typename T>
std::string containerToString(const T& container)
{
    std::string result = "";
    for (auto it = container.begin(); it!= container.end(); ++it) {
        if (it!= container.begin()) {
            result += ", ";
        }
        result += std::to_string(*it);
    }
    return result;
}

using TimePoint = std::chrono::time_point<std::chrono::system_clock>;

TimePoint getDeltaTimePoint(UInt64 milliseconds)
{
    return std::chrono::system_clock::now() + std::chrono::milliseconds(milliseconds);
}

}
