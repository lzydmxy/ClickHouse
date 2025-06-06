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

    ~WriteBufferFromBrpc() override { finish(); }

    void nextImpl() override
    {
        if (is_finished)
            throw Exception(ErrorCodes::CANNOT_WRITE_AFTER_END_OF_BUFFER, "WriteBufferFromBrpc is finished");
        resizeBufferBlock(buf.size() * size_multiplier);
    }

    void finish()
    {
        if (is_finished)
            return;
        is_finished = true;
        buf.resize(buf.size() - available());
        /// Prevent further writes.
        set(nullptr, 0);
    }

    const auto & getIntermediateBuf() const { return buf; }

    auto & getFinishedBuf()
    {
        finish();
        return buf;
    }
private:
    void resizeBufferBlock(size_t size)
    {
        auto prev_size = buf.size();
        auto ret = buf.resize(size);
        if(ret < 0)
            throw Exception(ErrorCodes::CANNOT_CREATE_IO_BUFFER, "Cannot resize butil::IOBuf to {}", size);
        auto block_num = buf.backing_block_num();
        if(block_num != 1)
            throw Exception(ErrorCodes::CANNOT_CREATE_IO_BUFFER, "Invalid block number {} in butil::IOBuf", block_num);
        auto block_view = buf.backing_block(block_num - 1);
        set(const_cast<Position>(block_view.data() + offset()), block_view.size() - prev_size);
#ifndef NDEBUG
        // auto curr_size = buf.size();
        // LOG_TRACE(getLogger("WriteBufferFromBrpc"), "WriteBufferFromBrpc initial_size {} multiplier {} block_num {}, resize {} to {} total {}, new block size {}",
        //     initial_size, size_multiplier, block_num,
        //     prev_size, size, curr_size, block_view.size() - prev_size);
#endif
    }
    static constexpr size_t initial_size = 32;
    static constexpr size_t size_multiplier = 2;
    butil::IOBuf buf;
    bool is_finished = false;
};

}
