#pragma once
#include <Core/ColumnNumbers.h>
#include <Functions/IFunction.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <DataTypes/IDataType.h>
#include <Query/Exchange/ExchangeUtils.h>
#include <Query/Exchange/IExchangeSink.h>
#include <Query/Exchange/ExchangeBufferedSender.h>
#include <Query/Exchange/DataTrans/DataTrans_fwd.h>

namespace DB
{
/// Send data to all partititons. Usually used with ResizeProcessor
///                   ||-> MultiPartitionExchangeSink
/// ResizeProcessor-->||-> MultiPartitionExchangeSink
///                   ||-> MultiPartitionExchangeSink
/// This pipeline will not keep data order and maximize the performance.
class MultiPartitionExchangeSink : public IExchangeSink
{
public:
    explicit MultiPartitionExchangeSink(
        Block header_,
        BroadcastSenderPtrs partition_senders_,
        ExecutableFunctionPtr repartition_func_,
        ColumnNumbers repartition_keys,
        ExchangeOptions options_,
        const String &name_);
    virtual String getName() const override { return name; }
    virtual void onCancel() override;
    virtual ~MultiPartitionExchangeSink() override = default;

    static String generateName(size_t exchange_id)
    {
        return fmt::format("MultiPartitionExchangeSink[{}]", exchange_id);
    }

    static String generateNameForTest()
    {
        return fmt::format("MultiPartitionExchangeSink[{}]", -1);
    }


protected:
    virtual void consume(Chunk) override;
    virtual void onFinish() override;

private:
    String name;
    const Block & header;
    BroadcastSenderPtrs partition_senders;
    size_t partition_num;
    size_t column_num;
    ExecutableFunctionPtr repartition_func;
    const ColumnNumbers repartition_keys;
    ExchangeOptions options;
    ExchangeBufferedSenders buffered_senders;
    ChunkInfoPtr current_chunk_info;
    LoggerPtr logger;
    const DataTypePtr * repartition_result_type_ptr ;
};

}
