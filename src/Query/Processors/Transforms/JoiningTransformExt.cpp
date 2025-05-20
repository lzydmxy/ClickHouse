#include <Query/Processors/Transforms/JoiningTransformExt.h>

#include <Query/Interpreters/JoinRuntimeFilterHelper.h>

#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ConcurrentHashJoin.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int CANNOT_READ_FROM_SOCKET;
}

JoiningTransformExt::JoiningTransformExt(
    const Block & input_header,
    const Block & output_header,
    JoinPtr join_,
    size_t max_block_size_,
    bool on_totals_,
    bool default_totals_,
    bool join_parallel_left_right_,
    FinishCounterPtr finish_counter_,
    size_t total_size_,
    size_t index_,
    FinishPipePtr finish_pipe_)
    : JoiningTransform(input_header, output_header, join_, max_block_size_, on_totals_, default_totals_, finish_counter_)
    , total_size(total_size_)
    , index(index_)
    , finish_pipe(std::move(finish_pipe_))
    , join_parallel_left_right(join_parallel_left_right_)
{
}

IProcessor::Status JoiningTransformExt::prepare()
{
    auto & output = outputs.front();
    auto & on_finish_output = outputs.back();

    /// Check can output.
    if (output.isFinished() || stop_reading)
    {
        output.finish();
        on_finish_output.finish();
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        for (auto & input : inputs)
            input.setNotNeeded();
        return Status::PortFull;
    }

    /// Output if has data.
    if (has_output)
    {
        output.push(std::move(output_chunk));
        has_output = false;

        return Status::PortFull;
    }


    if (inputs.size() > 1)
    {
        auto & last_in = inputs.back();
        if (!last_in.isFinished())
        {
            last_in.setNeeded();
            if (last_in.hasData())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "No data is expected from second JoiningTransform port");

            return Status::NeedData;
        }
    }

    if (has_input)
        return Status::Ready;

    auto & input = inputs.front();
    if (input.isFinished())
    {
        if (process_non_joined)
        {
            if (non_joined_blocks && !has_counter_finished && join->getName() == "ConcurrentHashJoin")
                return Status::Async;
            return Status::Ready;
        }

        output.finish();
        on_finish_output.finish();
        return Status::Finished;
    }

    input.setNeeded();

    if (!input.hasData())
        return Status::NeedData;

    input_chunk = input.pull(true);
    has_input = true;
    return Status::Ready;
}

void JoiningTransformExt::work()
{
    if (has_input)
    {
        transform(input_chunk);
        output_chunk.swap(input_chunk);
        has_input = not_processed != nullptr;
        has_output = !output_chunk.empty();
    }
    else
    {
        if (!non_joined_blocks)
        {
            if (auto concurrent_join = std::dynamic_pointer_cast<ConcurrentHashJoin>(join))
            {
                if (!finish_counter)
                {
                    process_non_joined = false;
                    return;
                }
                if (finish_counter->isLast())
                {
                    for (size_t i = 0; i < total_size; i++)
                    {
                        /// Send something to pipe to wake worker.
                        uint64_t buf = 1;
                        while (-1 == write((*finish_pipe)[i].event_fd, &buf, sizeof(buf)))
                        {
                            if (errno == EAGAIN)
                                break;

                            if (errno != EINTR)
                                throw Exception(ErrorCodes::CANNOT_READ_FROM_SOCKET, "Cannot write to pipe");
                        }
                    }
                    has_counter_finished = true;
                }

                concurrent_join->hash_joins[index]->data->getNonJoinedBlocks(inputs.front().getHeader(), outputs.front().getHeader(), max_block_size);
                if (!non_joined_blocks)
                {
                    process_non_joined = false;
                }
                return;
            }
            if (!finish_counter || !finish_counter->isLast())
            {
                process_non_joined = false;
                return;
            }

            non_joined_blocks = join->getNonJoinedBlocks(
                inputs.front().getHeader(), outputs.front().getHeader(), max_block_size);
            if (!non_joined_blocks)
            {
                process_non_joined = false;
                return;
            }
        }

        if (!has_counter_finished && finish_counter->finished.load() < finish_counter->total)
        {
            return;
        }
        else
        {
            has_counter_finished = true;
        }

        Block block = non_joined_blocks->next();
        if (!block)
        {
            process_non_joined = false;
            return;
        }

        auto rows = block.rows();
        output_chunk.setColumns(block.getColumns(), rows);
        has_output = true;
    }
}

JoiningTransformExt::~JoiningTransformExt()
{
    if (finish_pipe && finish_pipe->size() == total_size && (*finish_pipe)[index].event_fd != -1)
        close((*finish_pipe)[index].event_fd);
}

FillingRightJoinSideTransformExt::FillingRightJoinSideTransformExt(
    Block input_header, JoinPtr join_, JoiningTransform::FinishCounterPtr finish_counter_)
    : FillingRightJoinSideTransform(input_header, join_), finish_counter(std::move(finish_counter_))
{
}

IProcessor::Status FillingRightJoinSideTransformExt::prepare()
{
    auto & output = outputs.front();

    /// Check can output.
    if (output.isFinished())
    {
        for (auto & input : inputs)
            input.close();

        return Status::Finished;
    }

    if (!output.canPush())
    {
        for (auto & input : inputs)
            input.setNotNeeded();
        return Status::PortFull;
    }

    auto & input = inputs.front();

    if (stop_reading)
    {
        input.close();
    }
    else if (!input.isFinished())
    {
        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        chunk = input.pull(true);
        return Status::Ready;
    }

    if (inputs.size() > 1)
    {
        auto & totals_input = inputs.back();
        if (!totals_input.isFinished())
        {
            totals_input.setNeeded();

            if (!totals_input.hasData())
                return Status::NeedData;

            chunk = totals_input.pull(true);
            for_totals = true;
            return Status::Ready;
        }
    }
    else if (!set_totals)
    {
        chunk.setColumns(inputs.front().getHeader().cloneEmpty().getColumns(), 0);
        for_totals = true;
        return Status::Ready;
    }

    if (!should_build_runtime_filters && finish_counter && finish_counter->isLast())
    {
        should_build_runtime_filters = true;
        return Status::Ready;
    }

    output.finish();
    return Status::Finished;
}

void FillingRightJoinSideTransformExt::work()
{
    if (should_build_runtime_filters)
    {
        JoinRuntimeFiltersHelper::tryBuildRuntimeFilters(join);
        return;
    }

    auto block = inputs.front().getHeader().cloneWithColumns(chunk.detachColumns());

    if (for_totals)
        join->setTotals(block);
    else
        stop_reading = !join->addBlockToJoin(block);

    set_totals = for_totals;
}

}
