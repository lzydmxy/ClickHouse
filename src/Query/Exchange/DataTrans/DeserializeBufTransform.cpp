#include "DeserializeBufTransform.h"
#include <Columns/IColumn.h>
#include <IO/ReadBuffer.h>
#include <Compression/CompressedReadBuffer.h>
#include <Interpreters/Context_fwd.h>
#include <Query/Exchange/ExchangeUtils.h>
#include <Query/Exchange/bRPC/ReadBufferFromBrpc.h>
#include <Query/Exchange/DataTrans/NativeChunkInputStream.h>
#include <Query/Exchange/DataTrans/IBroadcastReceiver.h>

namespace DB
{
DeserializeBufTransform::DeserializeBufTransform(const Block & header_, bool enable_block_compress_)
    : ISimpleTransform(Block(), header_, true)
    , header(getOutputPort().getHeader())
    , enable_block_compress(enable_block_compress_)
    , logger(getLogger("DeserializeBufTransform"))
{
}

void DeserializeBufTransform::transform(Chunk & chunk)
{
    const ChunkInfoPtr & info = chunk.getChunkInfo();
    if (!info)
        return;

    auto iobuf_info = std::dynamic_pointer_cast<const DeserializeBufTransform::IOBufChunkInfo>(info);
    if (!iobuf_info)
        return;

    auto read_buffer = std::make_unique<ReadBufferFromBrpc>(iobuf_info->io_buf);
    std::unique_ptr<ReadBuffer> buf;
    if (enable_block_compress)
        buf = std::make_unique<CompressedReadBuffer>(*read_buffer);
    else
        buf = std::move(read_buffer);
    s.restart();
    NativeChunkInputStream chunk_in(*buf, header);
    chunk = chunk_in.readImpl();
    if (const auto * io_buf_with_receiver = dynamic_cast<const DeserializeBufTransform::IOBufChunkInfoWithReceiver *>(iobuf_info.get()))
    {
        if (auto receiver = io_buf_with_receiver->receiver.lock())
            receiver->addToMetricsMaybe(0, s.elapsedMilliseconds(), 0, chunk);
    }
}
}
