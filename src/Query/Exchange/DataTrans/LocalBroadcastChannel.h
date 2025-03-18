#pragma once
#include <atomic>
#include <cstddef>
#include <boost/noncopyable.hpp>
#include <base/types.h>
#include <Common/logger_useful.h>
#include <Processors/Chunk.h>
#include <Query/Common/QueryCommon.h>
#include <Query/Common/BoundedDataQueue.h>
#include <Query/Common/MultiPathBoundedQueue.h>
#include <Query/Exchange/QueryExchangeLog.h>
#include <Query/Exchange/ExchangeDataKey.h>
#include <Query/Exchange/ExchangeUtils.h>
#include <Query/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Query/Exchange/DataTrans/IBroadcastSender.h>

namespace DB
{

class LocalBroadcastChannel final : public IBroadcastReceiver,
                                    public IBroadcastSender,
                                    public std::enable_shared_from_this<LocalBroadcastChannel>,
                                    boost::noncopyable
{
public:
    LocalBroadcastChannel(
        ExchangeDataKeyPtr data_key_,
        LocalChannelOptions options_,
        const String & name_);

    LocalBroadcastChannel(
        ExchangeDataKeyPtr data_key_,
        LocalChannelOptions options_,
        const String & name_,
        MultiPathQueuePtr queue_,
        ContextPtr context_ = nullptr);

    BroadcastStatus sendImpl(Chunk chunk) override;
    RecvDataPacket recv(TimePoint timeout_ms) override;
    void registerToSenders(UInt32 timeout_ms) override;
    void merge(IBroadcastSender &&) override;
    String getName() const override;
    BroadcastStatus finish(BroadcastStatusCode status_code, String message) override;

    BroadcastSenderType getType() override { return BroadcastSenderType::Local; }

    ~LocalBroadcastChannel() override;

    static String generateName(
        size_t exchange_id, size_t write_segment_id, size_t read_segment_id, size_t parallel_index, String& co_host_port)
    {
        return fmt::format(
            "Local[{}_{}_{}_{}_{}]",
            write_segment_id,
            read_segment_id,
            parallel_index,
            exchange_id,
            co_host_port
        );
    }

    static String generateNameForTest()
    {
        return fmt::format(
            "Local[{}_{}_{}_{}_{}]",
            "test-Local", -1, -1, -1, -1
        );
    }

private:
    String name;
    ExchangeDataKeyPtr data_key;
    LocalChannelOptions options;
    MultiPathQueuePtr receive_queue;
    BroadcastStatus init_status{BroadcastStatusCode::RUNNING, false, "init"};
    std::atomic<BroadcastStatus *> broadcast_status{&init_status};
    ContextPtr context;
    LoggerPtr logger;
};
}
