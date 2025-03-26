#pragma once

#include <Processors/IProcessor.h>
#include <Processors/Transforms/JoiningTransform.h>


namespace DB
{

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

class NotJoinedBlocks;
class IBlocksStream;
using IBlocksStreamPtr = std::shared_ptr<IBlocksStream>;


class JoiningTransformExt : public JoiningTransform
{
public:
    struct EventFdStruct { int event_fd;};
    using FinishPipePtr = std::shared_ptr<std::vector<EventFdStruct>>;

    JoiningTransformExt(
        const Block & input_header,
        const Block & output_header,
        JoinPtr join_,
        size_t max_block_size_,
        bool on_totals_ = false,
        bool default_totals_ = false,
        bool join_parallel_left_right_ = true,
        FinishCounterPtr finish_counter_ = nullptr,
        size_t total_size_ = 0,
        size_t index_ = 0,
        FinishPipePtr finish_pipe_ = nullptr);

    ~JoiningTransformExt() override;

    String getName() const override { return "JoiningTransform"; }

    OutputPort & getFinishedSignal();

    Status prepare() override;
    void work() override;
    int schedule() override { return (*finish_pipe)[index].event_fd; }

private:
    size_t total_size = 0;
    size_t index = 0;
    FinishPipePtr finish_pipe;
    bool has_counter_finished = false;

    // TODO impl parallel execute left input and right input
    bool join_parallel_left_right;
};

class FillingRightJoinSideTransformExt : public FillingRightJoinSideTransform
{
public:
    FillingRightJoinSideTransformExt(Block input_header, JoinPtr join_, JoiningTransform::FinishCounterPtr finish_counter_ = nullptr);
    String getName() const override { return "FillingRightJoinSideExt"; }

    Status prepare() override;
    void work() override;

private:
    JoiningTransform::FinishCounterPtr finish_counter = nullptr;
    // After output finish, let's start build runtime filters
    bool should_build_runtime_filters = false;
};

}
