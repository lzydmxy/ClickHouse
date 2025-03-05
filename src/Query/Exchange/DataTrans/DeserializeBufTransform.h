#pragma once
#include <butil/iobuf.h>
#include <Common/logger_useful.h>
#include <Common/Stopwatch.h>
#include <Processors/Chunk.h>
#include <Processors/ISimpleTransform.h>


namespace DB
{
class IBroadcastReceiver;
using BroadcastReceiverPtr = std::shared_ptr<IBroadcastReceiver>;
class DeserializeBufTransform : public ISimpleTransform
{
public:

    struct IOBufChunkInfo : public ChunkInfo
    {
        butil::IOBuf io_buf;
    };

    struct IOBufChunkInfoWithReceiver : public IOBufChunkInfo
    {
        std::weak_ptr<IBroadcastReceiver> receiver;
    };

    explicit DeserializeBufTransform(const Block & header_, bool enable_block_compress_);

    String getName() const override { return "DeserializeBufTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    const Block & header;
    bool enable_block_compress;
    LoggerPtr logger;
    Stopwatch s;
};

}
