#include "ExchangeBufferedSender.h"
#include <Common/Exception.h>
#include <Query/Exchange/ExchangeUtils.h>
#include <Query/Exchange/ColumnSelector.h>
#include <Query/Exchange/DataTrans/IBroadcastSender.h>

namespace DB
{
ExchangeBufferedSender::ExchangeBufferedSender(
    const Block & header_, BroadcastSenderPtr sender_, UInt64 threshold_in_bytes_, UInt64 threshold_in_row_num_)
    : header(header_)
    , column_num(header_.getColumns().size())
    , sender(sender_)
    , threshold_in_bytes(threshold_in_bytes_)
    , threshold_in_row_num(threshold_in_row_num_)
    , logger(getLogger("ExchangeBufferedSender"))
{
    resetBuffer();
}

BroadcastStatus ExchangeBufferedSender::flush(bool force, const ChunkInfoPtr & chunk_info)
{
    size_t rows = partition_buffer[0]->size();

    LOG_TRACE(logger, "flush buffer, force: {}, row: {} threshold_in_row_num {}, memory(KB): {} threshold_in_bytes {}", 
        force, rows, threshold_in_row_num, bufferBytes() / 1024, threshold_in_bytes);

    if (rows == 0)
        return BroadcastStatus(BroadcastStatusCode::RUNNING);

    if (!force)
    {
        if (bufferBytes() < threshold_in_bytes && rows < threshold_in_row_num)
            return BroadcastStatus(BroadcastStatusCode::RUNNING);
    }

    LOG_TRACE(logger, "flush buffer, force: {}, row: {}, memory(KB): {}", force, rows, bufferBytes() / 1024);

    Chunk chunk(std::move(partition_buffer), rows, chunk_info);
    auto res = ExchangeUtils::sendAndCheckReturnStatus(*sender, std::move(chunk));
    resetBuffer();
    return res;
}

BroadcastStatus ExchangeBufferedSender::sendThrough(Chunk chunk)
{
    return ExchangeUtils::sendAndCheckReturnStatus(*sender, std::move(chunk));
}

void ExchangeBufferedSender::resetBuffer()
{
    partition_buffer = header.cloneEmptyColumns();
}

void ExchangeBufferedSender::append(
    size_t column_idx, MutableColumnPtr target)
{
    partition_buffer[column_idx] = std::move(target);
}

void ExchangeBufferedSender::appendSelective(size_t column_idx, const IColumn & source)
{
    partition_buffer[column_idx]->insertManyFrom(source, 0, source.size());
}

size_t ExchangeBufferedSender::bufferBytes() const
{
    size_t total = 0;
    for (size_t i = 0; i < column_num; ++i)
    {
        total += partition_buffer[i]->byteSize();
    }
    return total;
}

}
