#pragma once
#include <butil/iobuf.h>
#include <IO/WriteBuffer.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_CREATE_IO_BUFFER;
    extern const int CANNOT_WRITE_AFTER_END_OF_BUFFER;
}

/// Zero-copy write buffer from butil::IOBuf of brpc library.
/// Add a member IOBuf::epxand(size_t hint) for simplifying code, and very few performance gain
class WriteBufferFromBrpc : public WriteBuffer
{
public:
    WriteBufferFromBrpc() : WriteBuffer(nullptr, 0)
    {
        resizeBufferBlock(initial_size);
    }

    ~WriteBufferFromBrpc() override { finalize(); }

    void nextImpl() override
    {
        if (finalized)
            throw Exception(ErrorCodes::CANNOT_WRITE_AFTER_END_OF_BUFFER, "WriteBufferFromBrpc is finished");
        resizeBufferBlock(0);
    }

    void finalizeImpl() override
    {
        buf.resize(buf.size() - available());
        /// Prevent further writes.
        set(nullptr, 0);
    }

    const auto & getIntermediateBuf() const { return buf; }

    auto & getFinishedBuf()
    {
        finalize();
        return buf;
    }
private:
    void resizeBufferBlock(size_t size)
    {
        if (size == 0)
            size = buf.size();
        auto block_view = buf.expand(size);
        if (block_view.empty())
            throw Exception(ErrorCodes::CANNOT_CREATE_IO_BUFFER, "Cannot resize butil::IOBuf to {}" ,size);
        set(const_cast<Position>(block_view.data()), block_view.size());
#ifndef NDEBUG
        LOG_TRACE(getLogger("WriteBufferFromBrpc"), "WriteBufferFromBrpc initial_size {}, expand size {}, block view size {}, buff size {}",
            initial_size, size, block_view.size(), buf.size());
#endif
    }
    static constexpr size_t initial_size = 10240; //Default 10K
    butil::IOBuf buf;
};

}
