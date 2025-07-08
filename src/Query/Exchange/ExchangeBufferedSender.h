#pragma once
#include <vector>
#include <Common/logger_useful.h>
#include <Core/Block.h>
#include <Columns/IColumn.h>
#include <Processors/Chunk.h>
#include <Query/Exchange/DataTrans/DataTrans_fwd.h>

namespace DB
{
class ExchangeBufferedSender
{
public:
    ExchangeBufferedSender(const Block & header, BroadcastSenderPtr sender_, UInt64 threshold_in_bytes, UInt64 threshold_in_row_num);
    void append(size_t column_idx, MutableColumnPtr target);
    void appendSelective(size_t column_idx, const IColumn & source);
    BroadcastStatus sendThrough(Chunk chunk);
    BroadcastStatus flush(bool force, const ChunkInfoPtr & chunk_info);
private:
    const Block & header;
    size_t column_num;
    BroadcastSenderPtr sender;
    UInt64 threshold_in_bytes;
    UInt64 threshold_in_row_num;
    MutableColumns partition_buffer;
    LoggerPtr logger;
    void resetBuffer();
    inline size_t bufferBytes() const;
};

using ExchangeBufferedSenders = std::vector<ExchangeBufferedSender>;

}
