#pragma once

#include <Processors/IProcessor.h>

namespace DB
{

class BufferTransformExt : public IProcessor
{
public:
    explicit BufferTransformExt(const Block & header, size_t max_queue_size_ = std::numeric_limits<size_t>::max());

    String getName() const override { return "Buffer"; }
    Status prepare() override;

private:
    size_t max_queue_size;
    size_t max_used_queue_size = 0;
    size_t input_chunk_count = 0;
    std::list<Chunk> output_queue;
};

}
