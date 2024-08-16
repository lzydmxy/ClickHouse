#include <IO/WriteHelpers.h>
#include <Storages/Consensus/RaftConnectionStats.h>


namespace DB
{

uint64_t RaftConnectionStats::getMinLatency() const
{
    return min_latency;
}

uint64_t RaftConnectionStats::getMaxLatency() const
{
    return max_latency;
}

uint64_t RaftConnectionStats::getAvgLatency() const
{
    if (count != 0)
        return total_latency / count;
    return 0;
}

uint64_t RaftConnectionStats::getLastLatency() const
{
    return last_latency;
}

uint64_t RaftConnectionStats::getPacketsReceived() const
{
    return packets_received;
}

uint64_t RaftConnectionStats::getPacketsSent() const
{
    return packets_sent;
}

void RaftConnectionStats::incrementPacketsReceived()
{
    packets_received++;
}

void RaftConnectionStats::incrementPacketsSent()
{
    packets_sent++;
}

void RaftConnectionStats::updateLatency(uint64_t latency_ms)
{
    last_latency = latency_ms;
    total_latency += (latency_ms);
    count++;

    if (latency_ms < min_latency)
    {
        min_latency = latency_ms;
    }

    if (latency_ms > max_latency)
    {
        max_latency = latency_ms;
    }
}

void RaftConnectionStats::reset()
{
    resetLatency();
    resetRequestCounters();
}

void RaftConnectionStats::resetLatency()
{
    total_latency = 0;
    count = 0;
    max_latency = 0;
    min_latency = 0;
    last_latency = 0;
}

void RaftConnectionStats::resetRequestCounters()
{
    packets_received = 0;
    packets_sent = 0;
}

void RaftConnectionStats::dump(WriteBuffer & buf)
{
    writeText("(total count=", buf);
    writeIntText(count, buf);

    writeText(",recved=", buf);
    writeIntText(packets_received, buf);

    writeText(",sent=", buf);
    writeIntText(packets_sent, buf);

    writeText(",avg_latency=", buf);
    writeIntText(getAvgLatency(), buf);
    
    writeText(",min_latency=", buf);
    writeIntText(min_latency, buf);

    writeText(",max_latency=", buf);
    writeIntText(max_latency, buf);

    writeText(')', buf);
}

}
