#pragma once
#include <variant>
#include <butil/iobuf.h>
#include <bvar/reducer.h>
#include <sys/time.h>
#include <Common/DateLUT.h>
#include <Processors/Chunk.h>
#include <Query/Exchange/QueryExchangeLog.h>
#include <Query/Exchange/DataTrans/DataTrans_fwd.h>

namespace DB
{

using RecvDataPacket = std::variant<Chunk, BroadcastStatus>;
class IBroadcastReceiver
{
public:
    IBroadcastReceiver() : enable_receiver_metrics(false)
    {
    }
    explicit IBroadcastReceiver(bool enable_receiver_metrics_) : enable_receiver_metrics(enable_receiver_metrics_)
    {
    }
    struct ReceiverMetrics
    {
        bvar::Adder<size_t> recv_time_ms{};
        bvar::Adder<size_t> register_time_ms{};
        bvar::Adder<size_t> recv_rows{};
        bvar::Adder<size_t> recv_bytes{};
        bvar::Adder<size_t> recv_uncompressed_bytes{};
        bvar::Adder<size_t> recv_counts;
        bvar::Adder<size_t> dser_time_ms;
        std::atomic<Int32> finish_code{0};
        std::atomic<Int16> is_modifier{-1};
        String message;
    };
    virtual void registerToSenders(UInt32 timeout_ms) = 0;
    virtual RecvDataPacket recv(UInt32 timeout_ms)
    {
        UInt64 timeout_ms_ts = timeInMilliseconds(std::chrono::system_clock::now()) + timeout_ms;
        timespec timeout_ts {.tv_sec = long(timeout_ms_ts/1000), .tv_nsec = long(timeout_ms_ts % 1000) * 1000000};
        return recv(timeout_ts);
    }
    virtual RecvDataPacket recv(timespec timeout_ts) = 0;
    virtual BroadcastStatus finish(BroadcastStatusCode status_code, String message) = 0;
    virtual String getName() const = 0;
    virtual ~IBroadcastReceiver() = default;
    void setEnableReceiverMetrics(bool enable_) { enable_receiver_metrics = enable_; }

    bool enable_receiver_metrics = false;
    ReceiverMetrics receiver_metrics; // by default, metrics are disabled
    void addToMetricsMaybe(size_t recv_time_ms, size_t dser_time_ms, size_t recv_counts, const Chunk & chunk);
    void addToMetricsMaybe(size_t recv_time_ms, size_t dser_time_ms, size_t recv_counts, const butil::IOBuf & io_buf);
};

}
