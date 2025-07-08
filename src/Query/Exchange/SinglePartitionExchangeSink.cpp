#include "SinglePartitionExchangeSink.h"
#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <QueryPipeline/RemoteQueryExecutorReadContext.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Processors/ISource.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Query/Exchange/DataTrans/IBroadcastSender.h>
#include <Query/Exchange/RepartitionTransform.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

SinglePartitionExchangeSink::SinglePartitionExchangeSink(
    Block header_, BroadcastSenderPtr sender_, size_t partition_id_, ExchangeOptions options_, const String &name_)
    : IExchangeSink(std::move(header_))
    , name(name_)
    , header(getPort().getHeader())
    , sender(sender_)
    , partition_id(partition_id_)
    , column_num(header.columns())
    , options(options_)
    , buffered_sender(header, sender, options.send_threshold_in_bytes, options.send_threshold_in_row_num)
    , logger(getLogger("SinglePartitionExchangeSink"))
{
}

void SinglePartitionExchangeSink::consume(Chunk chunk)
{
    const ChunkInfoPtr & info = chunk.getChunkInfo();
    if (!info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk info was not set for chunk.");
    auto repartition_info = std::dynamic_pointer_cast<const RepartitionTransform::RepartitionChunkInfo>(info);
    if (!repartition_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk should have RepartitionChunkInfo .");

    const auto & chunk_info = repartition_info->origin_chunk_info;
    bool chunk_info_matched
        = ((current_chunk_info && chunk_info && *current_chunk_info == *chunk_info) || (!current_chunk_info && !chunk_info));
    LOG_TRACE(logger, "SinglePartitionExchangeSink consume, was_on_start_called {}, was_on_finish_called {}, is_finished {}, has_input {}",
        was_on_start_called, was_on_finish_called, is_finished.load(std::memory_order_relaxed), has_input);

    if (!chunk_info_matched)
    {
        buffered_sender.flush(true, current_chunk_info);
        current_chunk_info = chunk_info;
    }

    const IColumn::Selector & partition_selector = repartition_info->selector;

    const auto & columns = chunk.getColumns();
    LOG_TRACE(logger, "SinglePartitionExchangeSink append column {}", column_num);
    for (size_t i = 0; i < column_num; i++)
    {
        auto materialized_column = columns[i]->convertToFullColumnIfConst();
        auto columns = materialized_column->scatter(1, partition_selector);

        for(size_t j = 0; j < columns.size(); j++)
            buffered_sender.append(i, std::move(columns[j]));
    }
    auto status = buffered_sender.flush(false, current_chunk_info);
    LOG_TRACE(logger, "SinglePartitionExchangeSink status.code {}", toString(status.code));
    if (status.code != BroadcastStatusCode::RUNNING)
        onFinish();
}

void SinglePartitionExchangeSink::onFinish()
{
    LOG_TRACE(logger, "SinglePartitionExchangeSink on finish");
    buffered_sender.flush(true, current_chunk_info);
    IExchangeSink::onFinish();
}

void SinglePartitionExchangeSink::onCancel()
{
    LOG_TRACE(logger, "SinglePartitionExchangeSink on cancel");
    sender->finish(BroadcastStatusCode::SEND_CANCELLED, "Cancelled by pipeline");
}

}
