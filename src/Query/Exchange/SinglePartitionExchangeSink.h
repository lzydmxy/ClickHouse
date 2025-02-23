#pragma once
#include <Common/logger_useful.h>
#include <Core/ColumnNumbers.h>
#include <Functions/IFunction.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Query/Exchange/DataTrans/DataTrans_fwd.h>
#include <Query/Exchange/ExchangeBufferedSender.h>
#include <Query/Exchange/ExchangeUtils.h>
#include <Query/Exchange/IExchangeSink.h>

namespace DB
{
/// Send data to single partititon. Usually used with RepartitionTransform and BufferedCopyTransform:
///                                                 ||-> SinglePartitionExchangeSink[partition 0]
/// RepartitionTransform--> BufferedCopyTransform-->||-> SinglePartitionExchangeSink[partition 1]
///                                                 ||-> SinglePartitionExchangeSink[partition 2]
/// This pipeline can keep data order and maximize the parallelism.
class SinglePartitionExchangeSink : public IExchangeSink
{
public:
    explicit SinglePartitionExchangeSink(Block header_, 
        BroadcastSenderPtr sender_,
        size_t partition_id_,
        ExchangeOptions options_,
        const String &name_);
    String getName() const override { return name; }
    void onCancel() override;
    virtual ~SinglePartitionExchangeSink() override = default;

    static String generateName(size_t exchange_id)
    {
        return fmt::format("SinglePartitionExchangeSink[{}]", exchange_id);
    }

    static String generateNameForTest()
    {
        return fmt::format("SinglePartitionExchangeSink[{}]", -1);
    }

protected:
    void consume(Chunk) override;
    void onFinish() override;

private:
    String name;
    const Block & header;
    BroadcastSenderPtr sender;
    size_t partition_id;
    size_t column_num;
    ExchangeOptions options;
    ExchangeBufferedSender buffered_sender;
    ChunkInfoPtr current_chunk_info;
    LoggerPtr logger;
};

}
