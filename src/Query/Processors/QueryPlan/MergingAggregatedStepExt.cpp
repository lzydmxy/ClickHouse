#include <Query/Processors/QueryPlan/MergingAggregatedStepExt.h>

#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/MergingAggregatedTransform.h>

#include <DataTypes/DataTypesNumber.h>

namespace DB
{

MergingAggregatedStepExt::MergingAggregatedStepExt(
    const DataStream & input_stream_,
    Names keys_,
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
    , keys(std::move(keys_))
    , grouping_sets_params(std::move(grouping_sets_params_))
    , groupings(std::move(groupings_))
{
    NameSet output_names;
    for (const auto & key : keys)
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

    const auto & settings = build_settings.getBuildQueryPipelineSettingsExt().context->getSettingsRef();

    std::shared_ptr<AggregatingTransformParams> transform_params = nullptr;

    // optimizer use MergingAggregateStep in by-name style, regenerate aggregator params of by-position style
    if (!keys.empty())
    {
        ColumnNumbers key_positions;
        const auto & header = pipeline.getHeader();
        for (const auto & key : keys)
            key_positions.emplace_back(header.getPositionByName(key));

        transform_params = std::make_shared<AggregatingTransformParams>(pipeline.getHeader(),
            Aggregator::Params(keys, params.aggregates, params.overflow_row, settings.max_threads, settings.max_block_size, settings.min_hit_rate_to_use_consecutive_keys_optimization) , final);
    }
    else
    {
        transform_params = std::make_shared<AggregatingTransformParams>(pipeline.getHeader(), std::move(params), final);
    }


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

    computeGroupingFunctions(pipeline, groupings, keys, grouping_sets_params, build_settings);

    pipeline.resize(should_produce_results_in_order_of_bucket_number ? 1 : max_threads);
}

std::shared_ptr<IQueryPlanStep> MergingAggregatedStepExt::copy(ContextPtr) const
{
    return std::make_shared<MergingAggregatedStepExt>(input_streams[0], keys, grouping_sets_params, groupings, final, params, memory_efficient_aggregation, max_threads,
        memory_efficient_merge_threads, max_block_size, memory_bound_merging_max_block_bytes, group_by_sort_description, memory_bound_merging_of_aggregation_results_enabled);
}


}
