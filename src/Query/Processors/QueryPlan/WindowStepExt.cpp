#include <Query/Processors/QueryPlan/WindowStepExt.h>

#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Functions/FunctionsLogical.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/FinishSortingTransform.h>
#include <Processors/Transforms/LimitsCheckingTransform.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <Processors/Transforms/ScatterByPartitionTransform.h>
#include <Processors/Transforms/WindowTransform.h>

#include <Query/Common/OptimizerContext.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>
#include <Query/Processors/QueryPlan/MergeSortingStepExt.h>
#include <Query/Processors/QueryPlan/MergingSortedStepExt.h>
#include <Query/Processors/QueryPlan/PartialSortingStepExt.h>
#include <Query/Processors/QueryPlan/BuildQueryPipelineSettingsExt.h>

namespace DB
{
static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits{
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {.preserves_number_of_rows = true}};
}

static Block addWindowFunctionResultColumns(const Block & block, std::vector<WindowFunctionDescription> window_functions)
{
    auto result = block;

    for (const auto & f : window_functions)
    {
        ColumnWithTypeAndName column_with_type;
        column_with_type.name = f.column_name;
        column_with_type.type = f.aggregate_function->getResultType();
        column_with_type.column = column_with_type.type->createColumn();

        result.insert(column_with_type);
    }

    return result;
}

WindowStepExt::WindowStepExt(
    const DataStream & input_stream_, const WindowDescription & window_description_, bool need_sort_, SortDescription prefix_description_)
    : WindowStepExt(input_stream_, window_description_, window_description_.window_functions, need_sort_, prefix_description_)
{
}

WindowStepExt::WindowStepExt(
    const DataStream & input_stream_,
    const WindowDescription & window_description_,
    const std::vector<WindowFunctionDescription> & window_functions_,
    bool need_sort_,
    SortDescription prefix_description_)
    : ITransformingStep(input_stream_, addWindowFunctionResultColumns(input_stream_.header, window_functions_), getTraits())
    , window_description(window_description_)
    , window_functions(window_functions_)
    , need_sort(need_sort_)
    , prefix_description(prefix_description_)
{
    // We don't remove any columns, only add, so probably we don't have to update
    // the output DataStream::distinct_columns.

    window_description.checkValid();
}

void WindowStepExt::updateOutputStream()
{
    output_stream = createOutputStream(
        input_streams.front(), addWindowFunctionResultColumns(input_streams.front().header, window_functions), getDataStreamTraits());

    window_description.checkValid();
}

void WindowStepExt::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & s)
{
    const auto & settings_ext = BuildQueryPipelineSettingsExt::cast(s);
    auto enable_windows_parallel = settings_ext.context->getOptimizerContext()->getSettingsRef().enable_windows_parallel;
    if (need_sort && !window_description.full_sort_description.empty())
    {
        if (enable_windows_parallel)
            scatterByPartitionIfNeeded(pipeline);

        // finish sorting

        DataStream input_stream = input_streams[0];
        if (!prefix_description.empty() && !enable_windows_parallel)
        {
            bool need_finish_sorting = (prefix_description.size() < window_description.full_sort_description.size());
            if (pipeline.getNumStreams() > 1)
            {
                auto transform = std::make_shared<MergingSortedTransform>(
                    pipeline.getHeader(), pipeline.getNumStreams(), prefix_description, settings_ext.context->getSettingsRef().max_block_size, 0, SortingQueueStrategy::Batch);

                pipeline.addTransform(std::move(transform));
            }

            if (need_finish_sorting)
            {
                pipeline.addSimpleTransform(
                    [&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
                    {
                        if (stream_type != QueryPipelineBuilder::StreamType::Main)
                            return nullptr;

                        return std::make_shared<PartialSortingTransform>(header, window_description.full_sort_description, 0);
                    });

                /// NOTE limits are not applied to the size of temporary sets in FinishSortingTransform
                pipeline.addSimpleTransform(
                    [&](const Block & header) -> ProcessorPtr
                    {
                        return std::make_shared<FinishSortingTransform>(
                            header,
                            prefix_description,
                            window_description.full_sort_description,
                            settings_ext.context->getSettingsRef().max_block_size,
                            0,
                            FunctionsLogicalDetail::Ternary::True);
                    });
            }
        }
        else
        {
            PartialSortingStepExt partial_sorting_step{input_stream, window_description.full_sort_description, 0};
            partial_sorting_step.transformPipeline(pipeline, s);

            MergeSortingStepExt merge_sorting_step{input_stream, window_description.full_sort_description, 0};
            merge_sorting_step.transformPipeline(pipeline, s);
        }
        if (!enable_windows_parallel || window_description.partition_by.empty())
        {
            MergingSortedStepExt merging_sorted_step{
                input_stream, window_description.full_sort_description, settings_ext.context->getSettingsRef().max_block_size, 0};
            merging_sorted_step.transformPipeline(pipeline, s);
        }
    }

    // This resize is needed for cases such as `over ()` when we don't have a
    // sort node, and the input might have multiple streams. The sort node would
    // have resized it.
    if (!enable_windows_parallel || window_description.full_sort_description.empty())
        pipeline.resize(1);

    pipeline.addSimpleTransform(
        [&](const Block & /*header*/)
        {
            // TODO: hongzhigao1, WindowTransformExt
            return std::make_shared<WindowTransform>(
                input_streams.front().header, output_stream->header, window_description, window_functions);
        });

    assertBlocksHaveEqualStructure(
        pipeline.getHeader(), output_stream->header, "WindowStepExt transform for '" + window_description.window_name + "'");
}

void WindowStepExt::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << "Window: (";
    if (!window_description.partition_by.empty())
    {
        settings.out << "PARTITION BY ";
        for (size_t i = 0; i < window_description.partition_by.size(); ++i)
        {
            if (i > 0)
                settings.out << ", ";

            settings.out << window_description.partition_by[i].column_name;
        }
    }
    if (!window_description.partition_by.empty() && !window_description.order_by.empty())
        settings.out << " ";
    if (!window_description.order_by.empty())
        settings.out << "ORDER BY " << dumpSortDescription(window_description.order_by);
    settings.out << ")\n";

    for (size_t i = 0; i < window_functions.size(); ++i)
    {
        settings.out << prefix << (i == 0 ? "Functions: " : "           ");
        settings.out << window_functions[i].column_name << "\n";
    }
}

void WindowStepExt::describeActions(JSONBuilder::JSONMap & map) const
{
    if (!window_description.partition_by.empty())
    {
        auto partion_columns_array = std::make_unique<JSONBuilder::JSONArray>();
        for (const auto & descr : window_description.partition_by)
            partion_columns_array->add(descr.column_name);

        map.add("Partition By", std::move(partion_columns_array));
    }

    if (!window_description.order_by.empty())
        map.add("Sort Description", explainSortDescription(window_description.order_by));

    auto functions_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & func : window_functions)
        functions_array->add(func.column_name);

    map.add("Functions", std::move(functions_array));
}

std::shared_ptr<IQueryPlanStep> WindowStepExt::copy(ContextPtr) const
{
    return std::make_shared<WindowStepExt>(input_streams[0], window_description, window_functions, need_sort);
}

void WindowStepExt::toProto(Protos::WindowStepExt & proto, bool) const
{
    ProtosSerDerHelper::serializeToProtoBase(*this, *proto.mutable_query_plan_base());
    ProtosSerDerHelper::toProto(this->window_description, *proto.mutable_window_description());
    for (const auto & element : window_functions)
        ProtosSerDerHelper::toProto(element, *proto.add_window_functions());
    proto.set_need_sort(need_sort);
    for (const auto & element : prefix_description)
        ProtosSerDerHelper::toProto(element, *proto.add_prefix_description());
}

std::shared_ptr<WindowStepExt> WindowStepExt::fromProto(const Protos::WindowStepExt & proto, ContextPtr)
{
    auto [step_description, base_input_stream] = ProtosSerDerHelper::deserializeFromProtoBase(proto.query_plan_base());
    WindowDescription window_description = *ProtosSerDerHelper::fillFromProto(proto.window_description());;
    std::vector<WindowFunctionDescription> window_functions;
    for (const auto & proto_element : proto.window_functions())
    {
        WindowFunctionDescription element = *ProtosSerDerHelper::fillFromProto(proto_element);
        window_functions.emplace_back(std::move(element));
    }
    auto need_sort = proto.need_sort();
    SortDescription prefix_description;
    for (const auto & proto_element : proto.prefix_description())
    {
        SortColumnDescription element;
        ProtosSerDerHelper::fillFromProto(element, proto_element);
        prefix_description.emplace_back(std::move(element));
    }
    auto step = std::make_shared<WindowStepExt>(base_input_stream, window_description, window_functions, need_sort, prefix_description);
    step->setStepDescription(step_description);
    return step;
}

void WindowStepExt::scatterByPartitionIfNeeded(QueryPipelineBuilder & pipeline)
{
    size_t threads = pipeline.getNumThreads();
    size_t streams = pipeline.getNumStreams();

    if (!window_description.partition_by.empty() && threads > 1)
    {
        Block stream_header = pipeline.getHeader();

        ColumnNumbers key_columns;
        key_columns.reserve(window_description.partition_by.size());
        for (auto & col : window_description.partition_by)
            key_columns.push_back(stream_header.getPositionByName(col.column_name));

        pipeline.transform(
            [&](OutputPortRawPtrs ports)
            {
                Processors processors;
                for (auto * port : ports)
                {
                    auto scatter = std::make_shared<ScatterByPartitionTransform>(stream_header, threads, key_columns);
                    connect(*port, scatter->getInputs().front());
                    processors.push_back(scatter);
                }
                return processors;
            });

        if (streams > 1)
        {
            pipeline.transform(
                [&](OutputPortRawPtrs ports)
                {
                    Processors processors;
                    for (size_t i = 0; i < threads; ++i)
                    {
                        size_t output_it = i;
                        auto resize = std::make_shared<ResizeProcessor>(ports[output_it]->getHeader(), streams, 1);
                        auto & inputs = resize->getInputs();

                        for (auto input_it = inputs.begin(); input_it != inputs.end(); output_it += threads, ++input_it)
                            connect(*ports[output_it], *input_it);
                        processors.push_back(resize);
                    }
                    return processors;
                });
        }
    }
}

}
