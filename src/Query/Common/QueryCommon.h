#pragma once
#include <set>
#include <string>
#include <Common/DateLUT.h>
#include <Poco/Timespan.h>

/// Version of ClickHouse inter server BRPC protocol.
/// It's not necessary to increase this version number in most cases
/// unless the serialization of plan segment has changed.
#define DBMS_BRPC_PROTOCOL_MAJOR_VERSION 2
#define DBMS_BRPC_PROTOCOL_MINOR_VERSION 4

namespace DB
{

using PlanNodeId = UInt32;
using TimePoint = std::chrono::time_point<std::chrono::system_clock>;

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

inline TimePoint getDeltaTimePoint(UInt64 milliseconds)
{
    return std::chrono::system_clock::now() + std::chrono::milliseconds(milliseconds);
}

inline UInt64 getDeltaTimeFromNow(UInt64 timestamp_ms)
{
    auto now = timeInMilliseconds(std::chrono::system_clock::now());
    return timestamp_ms >= now ? timestamp_ms - now : 0;
}

inline std::string timeToString(UInt64 timestamp)
{
    return DateLUT::serverTimezoneInstance().timeToString(timestamp);
}

inline std::string timeToString(TimePoint time_point)
{
    return DateLUT::serverTimezoneInstance().timeToString(timeInSeconds(time_point));
}

inline timespec chronoToTimespec(const TimePoint& tp)
{
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(tp.time_since_epoch());
    timespec ts;
    ts.tv_sec = std::chrono::duration_cast<std::chrono::seconds>(duration).count();
    ts.tv_nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration % std::chrono::seconds(1)).count();
    return ts;
}

inline TimePoint timespecToTimePoint(const timespec& ts) {
    auto duration = std::chrono::seconds(ts.tv_sec) + std::chrono::microseconds(ts.tv_nsec);
    return TimePoint(duration);
}

inline TimePoint timespanToTimePoint(const Poco::Timespan& ts) {
    TimePoint now = std::chrono::system_clock::now();
    auto duration = std::chrono::seconds(ts.totalSeconds()) +
                    std::chrono::microseconds(ts.useconds());
    return now + duration;
}

}
