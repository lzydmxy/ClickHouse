#include <Query/Processors/QueryPlan/MergingAggregatedStepExt.h>

#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/MergingAggregatedTransform.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>
#include <Query/Processors/QueryPlan/QueryPlanStepHelper.h>
#include <DataTypes/DataTypesNumber.h>
#include <Query/Processors/QueryPlan/BuildQueryPipelineSettingsExt.h>

namespace DB
{

MergingAggregatedStepExt::MergingAggregatedStepExt(
    const DataStream & input_stream_,
    GroupingSetsParamsExtList grouping_sets_params_,
    GroupingDescriptions groupings_,
    bool final_,
    Aggregator::Params params_,
    bool memory_efficient_aggregation_,
    size_t max_threads_,
    size_t memory_efficient_merge_threads_,
    size_t max_block_size_,
    size_t memory_bound_merging_max_block_bytes_,
    SortDescription group_by_sort_description_,
    bool memory_bound_merging_of_aggregation_results_enabled_)
    : MergingAggregatedStep(input_stream_, params_, final_, memory_efficient_aggregation_, max_threads_
        , memory_efficient_merge_threads_, (!final_ && memory_efficient_aggregation_), max_block_size_,
        memory_bound_merging_max_block_bytes_, group_by_sort_description_, memory_bound_merging_of_aggregation_results_enabled_)
    , grouping_sets_params(std::move(grouping_sets_params_))
    , groupings(std::move(groupings_))
{
    for (const auto & grouping: groupings)
        output_stream->header.insert({std::make_shared<DataTypeUInt64>(), grouping.output_name});

    NameSet output_names;
    for (const auto & key : params.keys)
        if (!output_names.emplace(key).second)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "duplicate group by key: {}", key);

    for (const auto & aggregate : params.aggregates)
        if (!output_names.emplace(aggregate.column_name).second)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "duplicate aggreagte function output name: {}", aggregate.column_name);
}

static Block appendGroupingColumns(Block header, const GroupingDescriptions & groupings)
{
    for (const auto & grouping: groupings)
        header.insert({std::make_shared<DataTypeUInt64>(), grouping.output_name});

    return header;
}

void MergingAggregatedStepExt::updateOutputStream()
{
    output_stream->header = appendGroupingColumns(params.getHeader(input_streams.front().header, final), groupings);
}

void MergingAggregatedStepExt::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & build_settings)
{
    if (!memory_efficient_aggregation)
    {
        pipeline.resize(1);
    }

    std::shared_ptr<AggregatingTransformParams> transform_params = nullptr;

    transform_params = std::make_shared<AggregatingTransformParams>(pipeline.getHeader(), std::move(params), final);

    // @FIXME: grouping sets + two-level aggregation is incompatible with memory efficient merge
    // see also: https://meego.feishu.cn/clickhousech/story/detail/14744099
    if (!memory_efficient_aggregation || input_streams.front().header.has("__grouping_set"))
    {
        /// We union several sources into one, paralleling the work.
        pipeline.resize(1);

        /// Now merge the aggregated blocks
        pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<MergingAggregatedTransform>(header, transform_params, max_threads);
        });
    }
    else
    {
        auto num_merge_threads = memory_efficient_merge_threads
                                 ? static_cast<size_t>(memory_efficient_merge_threads)
                                 : static_cast<size_t>(max_threads);

        pipeline.addMergingAggregatedMemoryEfficientTransform(transform_params, num_merge_threads);
    }

    computeGroupingFunctions(pipeline, groupings, params.keys, grouping_sets_params, build_settings);

    pipeline.resize(should_produce_results_in_order_of_bucket_number ? 1 : max_threads);
}

std::shared_ptr<IQueryPlanStep> MergingAggregatedStepExt::copy(ContextPtr) const
{
    return std::make_shared<MergingAggregatedStepExt>(input_streams[0], grouping_sets_params, groupings, final, params, memory_efficient_aggregation, max_threads,
        memory_efficient_merge_threads, max_block_size, memory_bound_merging_max_block_bytes, group_by_sort_description, memory_bound_merging_of_aggregation_results_enabled);
}

void MergingAggregatedStepExt::toProto(Protos::MergingAggregatedStepExt & proto, bool) const
{
    ProtosSerDerHelper::serializeToProtoBase(*this, *proto.mutable_query_plan_base());
    for (const auto & element : grouping_sets_params)
        element.toProto(*proto.add_grouping_sets_params());
    for (const auto & element : groupings)
        element.toProto(*proto.add_groupings());

    ProtosSerDerHelper::toProto(params, *proto.mutable_params());
    proto.set_memory_efficient_aggregation(memory_efficient_aggregation);
    proto.set_max_threads(max_threads);
    proto.set_memory_efficient_merge_threads(memory_efficient_merge_threads);

    proto.set_final(final);
    proto.set_max_block_size(max_block_size);
    proto.set_memory_bound_merging_max_block_bytes(memory_bound_merging_max_block_bytes);
    for (const auto & element : group_by_sort_description)
        ProtosSerDerHelper::toProto(element, *proto.add_group_by_sort_description());

    proto.set_overwritten_sort_scope( DataStreamSortScopeConverter::toProto(overwritten_sort_scope));
    proto.set_should_produce_results_in_order_of_bucket_number(should_produce_results_in_order_of_bucket_number);
    proto.set_memory_bound_merging_of_aggregation_results_enabled(memory_bound_merging_of_aggregation_results_enabled);
}

std::shared_ptr<MergingAggregatedStepExt> MergingAggregatedStepExt::fromProto(const Protos::MergingAggregatedStepExt & proto, ContextPtr context)
{
    auto [step_description, base_input_stream] = ProtosSerDerHelper::deserializeFromProtoBase(proto.query_plan_base());

    GroupingSetsParamsExtList grouping_sets_params;
    for (const auto & proto_element : proto.grouping_sets_params())
    {
        GroupingSetsParamsExt element;
        element.fillFromProto(proto_element);
        grouping_sets_params.emplace_back(std::move(element));
    }

    GroupingDescriptions groupings;
    for (const auto & proto_element : proto.groupings())
    {
        GroupingDescription element;
        element.fillFromProto(proto_element);
        groupings.emplace_back(std::move(element));
    }
    auto params = ProtosSerDerHelper::fromProto(proto.params(), context);

    SortDescription group_by_sort_description;
    for (const auto & element : proto.group_by_sort_description())
    {
        SortColumnDescription sort_column_description;
        ProtosSerDerHelper::fillFromProto(sort_column_description, element);
        group_by_sort_description.emplace_back(std::move(sort_column_description));
    }


    auto step = std::make_shared<MergingAggregatedStepExt>(
        base_input_stream, std::move(grouping_sets_params), std::move(groupings), proto.final(),
        std::move(params),proto.memory_efficient_aggregation(), proto.max_threads(),
        proto.memory_efficient_merge_threads(), proto.max_block_size(), proto.memory_bound_merging_max_block_bytes(),
        std::move(group_by_sort_description), proto.memory_bound_merging_of_aggregation_results_enabled());
    step->setStepDescription(step_description);
    return step;
}

}
