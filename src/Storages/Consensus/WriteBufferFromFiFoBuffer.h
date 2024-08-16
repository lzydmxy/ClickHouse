#pragma once

#include <Poco/FIFOBuffer.h>
#include <Core/Types.h>
#include <IO/WriteBuffer.h>


namespace DB
{

using Poco::FIFOBuffer;
using FIFOBufferPtr = std::shared_ptr<FIFOBuffer>;

class WriteBufferFromFiFoBuffer : public WriteBuffer
{
public:
    explicit WriteBufferFromFiFoBuffer(size_t size = initial_size);
    bool isFinished() const { return is_finished; }
    void finalizeImpl() override;
    FIFOBufferPtr getBuffer();
    ~WriteBufferFromFiFoBuffer() override;
private:
    static constexpr size_t initial_size = 32;
    static constexpr size_t size_multiplier = 2;
    FIFOBufferPtr buffer;
    bool is_finished = false;
    void nextImpl() override;
};

using WriteBufferFromFiFoBufferPtr = std::shared_ptr<WriteBufferFromFiFoBuffer>;

}
