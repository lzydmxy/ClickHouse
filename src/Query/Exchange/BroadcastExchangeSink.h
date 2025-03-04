#pragma once
#include <Common/logger_useful.h>
#include <Processors/IProcessor.h>
#include <Query/Exchange/BufferChunk.h>
#include <Query/Exchange/DataTrans/DataTrans_fwd.h>
#include <Query/Exchange/ExchangeUtils.h>
#include <Query/Exchange/IExchangeSink.h>

namespace DB
{

/// Sink which broadcast data to ExchangeSource.
class BroadcastExchangeSink : public IExchangeSink
{
public:
    BroadcastExchangeSink(Block header_, BroadcastSenderPtrs senders_, ExchangeOptions options_, const String &name_);
    virtual ~BroadcastExchangeSink() override;
    String getName() const override { return name; }
    BroadcastSenderPtrs getSenders() const
    {
        return senders;
    }

    static String generateName(size_t exchange_id)
    {
        return fmt::format("BroadcastExchangeSink[{}]", exchange_id);
    }

    static String generateNameForTest()
    {
        return fmt::format("BroadcastExchangeSink[{}]", -1);
    }

protected:
    virtual void consume(Chunk) override;
    virtual void onFinish() override;
    virtual void onCancel() override;

private:
    String name;
    BroadcastSenderPtrs senders;
    ExchangeOptions options;
    BufferChunk buffer_chunk;
    LoggerPtr logger;
};

}
