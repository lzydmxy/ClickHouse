#include <Query/Processors/Transforms/BufferTransformExt.h>
// #include <Common/logger_useful.h>

namespace DB
{

BufferTransformExt::BufferTransformExt(const Block & header, size_t max_queue_size_)
    : IProcessor(InputPorts(1, header), OutputPorts(1, header)), max_queue_size(max_queue_size_)
{
}

BufferTransformExt::Status BufferTransformExt::prepare()
{
    auto & input = inputs.front();
    auto & output = outputs.front();

    if (input.hasData())
    {
        if (output_queue.size() >= max_queue_size)
            return Status::PortFull;
        output_queue.push_back(input.pull());
        max_used_queue_size = std::max(max_used_queue_size, output_queue.size());
        ++input_chunk_count;
    }

    if (output.canPush())
    {
        if (output_queue.empty() && input.isFinished())
        {
            output.finish();
        }
        else if (!output_queue.empty())
        {
            output.push(std::move(output_queue.front()));
            output_queue.pop_front();
        }
    }

    input.setNeeded();

    if (input.isFinished() && output.isFinished())
    {
        // FIXME: rows, bytes are protected
        // LOG_DEBUG(
        //     getLogger("BufferTransform"),
        //     "max_used_queue_size:{}/{}, input:[rows:{} bytes:{}], output:[rows:{} bytes:{}]",
        //     max_used_queue_size,
        //     input_chunk_count,
        //     input.getRows(),
        //     input.getBytes(),
        //     output.getRows(),
        //     output.getBytes());
        return Status::Finished;
    }
    else if (input.isFinished())
    {
        return Status::PortFull;
    }
    else
    {
        return Status::NeedData;
    }
}

}
