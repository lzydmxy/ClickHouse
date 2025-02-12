#pragma once
#include <set>
#include <string>
#include <Common/DateLUT.h>

/// Version of ClickHouse inter server BRPC protocol.
/// It's not necessary to increase this version number in most cases
/// unless the serialization of plan segment has changed.
#define DBMS_BRPC_PROTOCOL_MAJOR_VERSION 2
#define DBMS_BRPC_PROTOCOL_MINOR_VERSION 4

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

inline TimePoint getDeltaTimePoint(UInt64 milliseconds)
{
    return std::chrono::system_clock::now() + std::chrono::milliseconds(milliseconds);
}

inline UInt64 getDeltaTimeFromNow(UInt64 timestamp_ms)
{
    auto now = timeInMilliseconds(std::chrono::system_clock::now());
    return timestamp_ms >= now ? timestamp_ms - now : 0;
}

}
