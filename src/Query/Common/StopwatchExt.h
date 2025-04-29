#pragma once

#include <Common/Stopwatch.h>

namespace DB
{


template <typename TStopwatch>
class StopwatchGuard : public TStopwatch
{
public:
    explicit StopwatchGuard(UInt64 & elapsed_ns_) : elapsed_ns(elapsed_ns_) {}

    ~StopwatchGuard() { elapsed_ns += TStopwatch::elapsedNanoseconds(); }

private:
    UInt64 & elapsed_ns;
};



}