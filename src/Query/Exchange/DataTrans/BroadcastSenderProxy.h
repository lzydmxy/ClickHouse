#pragma once
#include <boost/core/noncopyable.hpp>
#include <Common/logger_useful.h>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
#include <Query/Exchange/ExchangeDataKey.h>
#include <Query/Exchange/DataTrans/DataTrans_fwd.h>
#include <Query/Exchange/DataTrans/IBroadcastSender.h>

namespace DB
{
struct SenderProxyOptions
{
    uint64_t wait_timeout_ms;
};

class BroadcastSenderProxy final : public IBroadcastSender, boost::noncopyable
{
public:
    virtual ~BroadcastSenderProxy() override;
    BroadcastStatus sendImpl(Chunk chunk) override;
    BroadcastStatus finish(BroadcastStatusCode status_code, String message) override;
    void merge(IBroadcastSender && /*sender*/) override;
    bool needMetrics() override { return false; }
    String getName() const override;
    BroadcastSenderType getType() override;
    void accept(ContextPtr context_, Block header_);
    void waitAccept(UInt32 /*timeout_ms*/);

    void becomeRealSender(BroadcastSenderPtr sender);
    void waitBecomeRealSender(UInt32 /*timeout_ms*/);

    ContextPtr getContext() const;
    Block getHeader() const;
    ExchangeDataKeyPtr getDataKey() const;

    SenderMetrics & getSenderMetrics()
    {
        if (!has_real_sender.load(std::memory_order_relaxed))
            waitBecomeRealSender(wait_timeout_ms);
        return real_sender->getSenderMetrics();
    }

private:
    friend class BroadcastSenderProxyRegistry;
    explicit BroadcastSenderProxy(ExchangeDataKeyPtr data_key_, SenderProxyOptions options);

    mutable std::mutex mutex;
    std::condition_variable wait_become_real;
    std::condition_variable wait_accept;
    std::atomic_bool has_real_sender {false};
    bool closed {false};
    ExchangeDataKeyPtr data_key;

    ContextPtr context;
    Block header;
    BroadcastSenderPtr real_sender;

    UInt32 wait_timeout_ms;

    LoggerPtr logger;
};

}
