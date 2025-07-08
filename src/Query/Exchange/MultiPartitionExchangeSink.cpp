#include "MultiPartitionExchangeSink.h"
#include <Common/logger_useful.h>
#include <Columns/IColumn.h>
#include <Query/Exchange/RepartitionTransform.h>
#include <Query/Exchange/DataTrans/IBroadcastSender.h>

namespace DB
{
MultiPartitionExchangeSink::MultiPartitionExchangeSink(
    Block header_,
    BroadcastSenderPtrs partition_senders_,
    ExecutableFunctionPtr repartition_func_,
    ColumnNumbers repartition_keys_,
    ExchangeOptions options_,
    const String &name_)
    : IExchangeSink(std::move(header_))
    , name(name_)
    , header(getPort().getHeader())
    , partition_senders(std::move(partition_senders_))
    , partition_num(partition_senders.size())
    , column_num(header.columns())
    , repartition_func(std::move(repartition_func_))
    , repartition_keys(std::move(repartition_keys_))
    , options(options_)
    , logger(getLogger("MultiPartitionExchangeSink"))
{
    bool has_null_shuffle_key = false;
    for (size_t key_idx : repartition_keys)
    {
        const auto & type_and_name = header.safeGetByPosition(key_idx);
        if (type_and_name.type->isNullable())
        {
            has_null_shuffle_key = true;
            break;
        }
    }

    if (has_null_shuffle_key)
        repartition_result_type_ptr = &RepartitionTransform::REPARTITION_FUNC_NULLABLE_RESULT_TYPE;
    else
        repartition_result_type_ptr = &RepartitionTransform::REPARTITION_FUNC_RESULT_TYPE;

    for(size_t i = 0; i < partition_num; ++i)
    {
        ExchangeBufferedSender buffered_sender (header, partition_senders[i], options.send_threshold_in_bytes, options.send_threshold_in_row_num);
        buffered_senders.emplace_back(std::move(buffered_sender));
    }
}

void MultiPartitionExchangeSink::consume(Chunk chunk)
{
    if (partition_num == 1)
    {
        auto status = buffered_senders[0].sendThrough(std::move(chunk));
        if (status.code != BroadcastStatusCode::RUNNING)
            onFinish();
        return;
    }

    const auto & chunk_info = chunk.getChunkInfo();

    LOG_TRACE(logger, "MultiPartitionExchangeSink consume {} rows", chunk.getNumRows());

    bool chunk_info_matched
        = ((current_chunk_info && chunk_info && *current_chunk_info == *chunk_info) || (!current_chunk_info && !chunk_info));

    if (!chunk_info_matched)
    {
        for (size_t i = 0; i < partition_num; ++i)
        {
            buffered_senders[i].flush(true, current_chunk_info);
        }
        current_chunk_info = chunk_info;
    }

    IColumn::Selector partition_selector = RepartitionTransform::doRepartition(
        partition_num, chunk, header, repartition_keys, repartition_func, *repartition_result_type_ptr);

    const auto & columns = chunk.getColumns();
    for (size_t col_idx = 0; col_idx < column_num; col_idx++)
    {
         auto materialized_columns = columns[col_idx]->scatter(partition_num, partition_selector);
         for (size_t partition_idx = 0; partition_idx < partition_num; ++ partition_idx)
         {
             if (col_idx == 0)
                 LOG_TRACE(logger, "MultiPartitionExchangeSink repartition to {}, partition index {}, size {}", partition_num, partition_idx, materialized_columns[partition_idx]->size());
             if (materialized_columns[partition_idx]->size() == 0)
                 continue;
             buffered_senders[partition_idx].append(col_idx, std::move(materialized_columns[partition_idx]));
         }
    }

    bool has_active_sender = false;
    for (size_t i = 0; i < partition_num; ++i)
    {
        auto status = buffered_senders[i].flush(false, current_chunk_info);
        if (status.code == BroadcastStatusCode::RUNNING)
            has_active_sender = true;
    }
    if (!has_active_sender)
        onFinish();
}

void MultiPartitionExchangeSink::onFinish()
{
    LOG_TRACE(logger, "MultiPartitionExchangeSink on finish");
    for(size_t i = 0; i < partition_num ; ++i)
        buffered_senders[i].flush(true, current_chunk_info);
    IExchangeSink::onFinish();
}

void MultiPartitionExchangeSink::onCancel()
{
    LOG_TRACE(logger, "MultiPartitionExchangeSink on cancel");
    for (BroadcastSenderPtr & sender : partition_senders)
        sender->finish(BroadcastStatusCode::SEND_CANCELLED, "Cancelled by pipeline");
}

}
