#pragma once
#include <optional>
#include <base/types.h>
#include <bvar/reducer.h>
#include <Common/Stopwatch.h>
#include <Processors/Chunk.h>
#include <Query/Exchange/QueryExchangeLog.h>
#include <Query/Exchange/DataTrans/DataTrans_fwd.h>

namespace DB
{


enum class BroadcastSenderType
{
    Local = 0,
    Brpc = 1,
    Disk = 2
};
class IBroadcastSender
{
public:
    IBroadcastSender() : enable_sender_metrics(false)
    {
    }
    explicit IBroadcastSender(bool enable_sender_metrics_) : enable_sender_metrics(enable_sender_metrics_)
    {
    }
    /// metrics for sender, bvar is used for write intensive case
    struct SenderMetrics
    {
        /// all senders
        bvar::Adder<size_t> send_time_ms{};
        bvar::Adder<size_t> send_rows{};
        bvar::Adder<size_t> send_uncompressed_bytes{};
        bvar::Adder<size_t> num_send_times{};
        /// for brpc senders
        bvar::Adder<size_t> send_bytes{};
        bvar::Adder<size_t> ser_time_ms{};
        bvar::Adder<size_t> send_retry{};
        bvar::Adder<size_t> send_retry_ms{};
        bvar::Adder<size_t> overcrowded_retry{};
        /// finish related
        std::atomic<Int32> finish_code{};
        std::atomic<bool> is_modifier{false};
        String message;
    };
    BroadcastStatus send(Chunk chunk) noexcept;
    /// sendImpl should be thread-safe
    virtual BroadcastStatus sendImpl(Chunk chunk) = 0;
    /// finish should be thread-safe
    virtual BroadcastStatus finish(BroadcastStatusCode status_code, String message) = 0;
    /// Merge sender to get 1:N sender and can avoid duplicated serialization when send chunk
    virtual void merge(IBroadcastSender && sender) = 0;

    virtual String getName() const = 0;
    virtual BroadcastSenderType getType() = 0;
    virtual bool needMetrics() { return true; }
    virtual ~IBroadcastSender() = default;
    SenderMetrics & getSenderMetrics()
    {
        return sender_metrics;
    }

protected:
    bool enable_sender_metrics = false; // by default, metrics are disabled
    SenderMetrics sender_metrics;

private:
    Stopwatch sender_timer;
};

using BroadcastSenderPtr = std::shared_ptr<IBroadcastSender>;

}
