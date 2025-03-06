
#include <Query/Processors/QueryPlan/PartitionTopNStepExt.h>
#include <Query/Processors/Transforms/PartitionTopNTransformExt.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include "Core/ColumnNumbers.h"

namespace DB
{

PartitionTopNStepExt::PartitionTopNStepExt(
    const DataStream & input_stream_, const Names & partition_, const Names & order_by_, UInt64 limit_, TopNModel model_)
    : ITransformingStep(input_stream_, input_stream_.header, {}), partition(partition_), order_by(order_by_), limit(limit_), model(model_)
{
}

void PartitionTopNStepExt::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & context)
{
    auto input_header = pipeline.getHeader();
    // FIXME: No member named 'context' in 'DB::BuildQueryPipelineSettings'
    // pipeline.resize(context.context->getSettingsRef().max_threads);

    ColumnNumbers partition_by_columns;
    for (const auto & col : partition)
        partition_by_columns.emplace_back(input_header.getPositionByName(col));

    ColumnNumbers order_by_columns;
    for (const auto & col : order_by)
        order_by_columns.emplace_back(input_header.getPositionByName(col));

    pipeline.addSimpleTransform(
        [&](const Block & header)
        { return std::make_shared<PartitionTopNTransformExt>(header, limit, partition_by_columns, order_by_columns, model, true); });
}

void PartitionTopNStepExt::updateOutputStream()
{
    output_stream->header = input_streams[0].header;
}

std::shared_ptr<IQueryPlanStep> PartitionTopNStepExt::copy(ContextPtr) const
{
    return std::make_shared<PartitionTopNStepExt>(input_streams[0], partition, order_by, limit, model);
}

}
