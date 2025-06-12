#include "QueryPipelineBuilderHelper.h"

#include <Core/SortDescription.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/DelayedPortsProcessor.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Transforms/CreatingSetsTransform.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <Processors/Transforms/MergeJoinTransform.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <Query/Processors/Transforms/JoiningTransformExt.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>

#include <Common/typeid_cast.h>

#include <sys/eventfd.h>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_OPEN_FILE;
}

std::unique_ptr<QueryPipelineBuilder> QueryPipelineBuilderHelper::joinPipelinesWithRuntimeFilter(
    std::unique_ptr<QueryPipelineBuilder> left,
    std::unique_ptr<QueryPipelineBuilder> right,
    JoinPtr join,
    size_t max_block_size,
    size_t max_streams,
    bool keep_left_read_in_order,
    bool join_parallel_left_right,
    Processors * collected_processors,
    bool need_build_runtime_filter)
{
    left->checkInitializedAndNotCompleted();
    right->checkInitializedAndNotCompleted();

    /// Extremes before join are useless. They will be calculated after if needed.
    left->pipe.dropExtremes();
    right->pipe.dropExtremes();

    left->pipe.collected_processors = collected_processors;
    right->pipe.collected_processors = collected_processors;

    /// In case joined subquery has totals, and we don't, add default chunk to totals.
    bool default_totals = false;
    if (!left->hasTotals() && right->hasTotals())
    {
        left->addDefaultTotals();
        default_totals = true;
    }

    ///                                     (left) ──────┐
    ///                                                  ╞> Joining ─> (joined)
    ///                                     (left) ─┐┌───┘
    ///                                             └┼───┐
    /// (right) ┐                         (totals) ──┼─┐ ╞> Joining ─> (joined)
    ///         ╞> Resize ┐                        ╓─┘┌┼─┘
    /// (right) ┘         │                        ╟──┘└─┐
    ///                   ╞> FillingJoin ─> Resize ╣     ╞> Joining ─> (totals)
    /// (totals) ─────────┘                        ╙─────┘

    size_t num_streams = left->getNumStreams();

    if (join->supportParallelJoin() && !right->hasTotals())
    {
        if (!keep_left_read_in_order)
        {
            left->resize(max_streams);
            num_streams = max_streams;
        }

        right->resize(max_streams);
        std::shared_ptr<JoiningTransform::FinishCounter> finish_counter;
        if (need_build_runtime_filter)
            finish_counter = std::make_shared<JoiningTransform::FinishCounter>(max_streams);

        auto concurrent_right_filling_transform = [&](OutputPortRawPtrs outports)
        {
            Processors processors;
            for (auto & outport : outports)
            {
                auto adding_joined = std::make_shared<FillingRightJoinSideTransformExt>(right->getHeader(), join, finish_counter);
                connect(*outport, adding_joined->getInputs().front());
                processors.emplace_back(adding_joined);
            }
            return processors;
        };
        right->transform(concurrent_right_filling_transform);
        right->resize(1);
    }
    else
    {
        right->resize(1);
        std::shared_ptr<JoiningTransform::FinishCounter> finish_counter;
        if (need_build_runtime_filter)
            finish_counter = std::make_shared<JoiningTransform::FinishCounter>(1);

        auto adding_joined = std::make_shared<FillingRightJoinSideTransformExt>(right->getHeader(), join, finish_counter);
        InputPort * totals_port = nullptr;
        if (right->hasTotals())
            totals_port = adding_joined->addTotalsPort();

        right->addTransform(std::move(adding_joined), totals_port, nullptr);
    }

    size_t num_streams_including_totals = num_streams + (left->hasTotals() ? 1 : 0);
    right->resize(num_streams_including_totals);

    /// This counter is needed for every Joining except totals, to decide which Joining will generate non joined rows.
    auto finish_counter = std::make_shared<JoiningTransform::FinishCounter>(num_streams);
    auto finish_pipe = std::make_shared<std::vector<JoiningTransformExt::EventFdStruct>>();
    if (join->getName() == "ConcurrentHashJoin")
    {
        for (size_t i = 0; i < num_streams; i++)
        {
            finish_pipe->push_back({-1});
        }

        for (size_t i = 0; i < num_streams; i++)
        {
            int event_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
            if (-1 == event_fd)
                throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Cannot create event_fd for ConcurrentJoin");
            (*finish_pipe)[i].event_fd = event_fd;
        }
    }

    auto lit = left->pipe.output_ports.begin();
    auto rit = right->pipe.output_ports.begin();

    std::vector<OutputPort *> joined_output_ports;
    std::vector<OutputPort *> delayed_root_output_ports;

    std::shared_ptr<DelayedJoinedBlocksTransform> delayed_root = nullptr;
    if (join->hasDelayedBlocks())
    {
        delayed_root = std::make_shared<DelayedJoinedBlocksTransform>(num_streams, join);
        if (!delayed_root->getInputs().empty() || delayed_root->getOutputs().size() != num_streams)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "DelayedJoinedBlocksTransform should have no inputs and {} outputs, "
                            "but has {} inputs and {} outputs",
                            num_streams, delayed_root->getInputs().size(), delayed_root->getOutputs().size());

        if (collected_processors)
            collected_processors->emplace_back(delayed_root);
        left->pipe.processors->emplace_back(delayed_root);

        for (auto & outport : delayed_root->getOutputs())
            delayed_root_output_ports.emplace_back(&outport);
    }


    Block left_header = left->getHeader();
    Block joined_header = JoiningTransformExt::transformHeader(left_header, join);

    for (size_t i = 0; i < num_streams; ++i)
    {
        auto joining = std::make_shared<JoiningTransformExt>(left_header, join, max_block_size, false, default_totals, join_parallel_left_right, finish_counter, num_streams, i, finish_pipe);
        connect(**lit, joining->getInputs().front());
        connect(**rit, joining->getInputs().back());
        if (delayed_root)
        {
            // Process delayed joined blocks when all JoiningTransform are finished.
            auto delayed = std::make_shared<DelayedJoinedBlocksWorkerTransform>(
                joined_header,
                [left_header, joined_header, max_block_size, join]()
                { return join->getNonJoinedBlocks(left_header, joined_header, max_block_size); });
            if (delayed->getInputs().size() != 1 || delayed->getOutputs().size() != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "DelayedJoinedBlocksWorkerTransform should have one input and one output");

            connect(*delayed_root_output_ports[i], delayed->getInputs().front());

            joined_output_ports.push_back(&joining->getOutputs().front());
            joined_output_ports.push_back(&delayed->getOutputs().front());

            if (collected_processors)
                collected_processors->emplace_back(delayed);
            left->pipe.processors->emplace_back(std::move(delayed));
        }
        else
        {
            *lit = &joining->getOutputs().front();
        }

        ++lit;
        ++rit;

        if (collected_processors)
            collected_processors->emplace_back(joining);

        left->pipe.processors->emplace_back(std::move(joining));
    }

    if (delayed_root)
    {
        // Process DelayedJoinedBlocksTransform after all JoiningTransforms.
        DelayedPortsProcessor::PortNumbers delayed_ports_numbers;
        delayed_ports_numbers.reserve(joined_output_ports.size() / 2);
        for (size_t i = 1; i < joined_output_ports.size(); i += 2)
            delayed_ports_numbers.push_back(i);

        auto delayed_processor = std::make_shared<DelayedPortsProcessor>(joined_header, 2 * num_streams, delayed_ports_numbers);
        if (collected_processors)
            collected_processors->emplace_back(delayed_processor);
        left->pipe.processors->emplace_back(delayed_processor);

        // Connect @delayed_processor ports with inputs (JoiningTransforms & DelayedJoinedBlocksTransforms) / pipe outputs
        auto next_delayed_input = delayed_processor->getInputs().begin();
        for (OutputPort * port : joined_output_ports)
            connect(*port, *next_delayed_input++);
        left->pipe.output_ports.clear();
        for (OutputPort & port : delayed_processor->getOutputs())
            left->pipe.output_ports.push_back(&port);
        left->pipe.header = joined_header;
        left->resize(num_streams);
    }

    if (left->hasTotals())
    {
        auto joining = std::make_shared<JoiningTransformExt>(left_header, join, max_block_size, true, default_totals);
        connect(*left->pipe.totals_port, joining->getInputs().front());
        connect(**rit, joining->getInputs().back());
        left->pipe.totals_port = &joining->getOutputs().front();

        ++rit;

        if (collected_processors)
            collected_processors->emplace_back(joining);

        left->pipe.processors->emplace_back(std::move(joining));
    }

    left->pipe.processors->insert(left->pipe.processors->end(), right->pipe.processors->begin(), right->pipe.processors->end());
    left->pipe.header = left->pipe.output_ports.front()->getHeader();
    left->pipe.max_parallel_streams = std::max(left->pipe.max_parallel_streams, right->pipe.max_parallel_streams);
    return left;
}

}
