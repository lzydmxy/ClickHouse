
#include <Core/ColumnNumbers.h>
#include <Query/Processors/QueryPlan/PartitionTopNStepExt.h>
#include <Query/Processors/Transforms/PartitionTopNTransformExt.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Query/Processors/QueryPlan/BuildQueryPipelineSettingsExt.h>

namespace DB
{

PartitionTopNStepExt::PartitionTopNStepExt(
    const DataStream & input_stream_, const Names & partition_, const Names & order_by_, UInt64 limit_, TopNModel model_)
    : ITransformingStep(input_stream_, input_stream_.header, {}), partition(partition_), order_by(order_by_), limit(limit_), model(model_)
{
}

void PartitionTopNStepExt::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    const auto & settings_ext = BuildQueryPipelineSettingsExt::cast(settings);

    auto input_header = pipeline.getHeader();
    pipeline.resize(settings_ext.context->getSettingsRef().max_threads);

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

void PartitionTopNStepExt::toProto(Protos::PartitionTopNStepExt & proto, bool) const
{
    ProtosSerDerHelper::serializeToProtoBase(*this, *proto.mutable_query_plan_base());
    for (const auto & element : partition)
        proto.add_partition(element);
    for (const auto & element : order_by)
        proto.add_order_by(element);
    proto.set_limit(limit);
    proto.set_model(TopNModelConverter::toProto(model));
}

std::shared_ptr<PartitionTopNStepExt> PartitionTopNStepExt::fromProto(const Protos::PartitionTopNStepExt & proto, ContextPtr)
{
    auto [step_description, base_input_stream] = ProtosSerDerHelper::deserializeFromProtoBase(proto.query_plan_base());
    std::vector<String> partition;
    for (const auto & element : proto.partition())
        partition.emplace_back(element);
    std::vector<String> order_by;
    for (const auto & element : proto.order_by())
        order_by.emplace_back(element);
    auto limit = proto.limit();
    auto model = TopNModelConverter::fromProto(proto.model());
    auto step = std::make_shared<PartitionTopNStepExt>(base_input_stream, partition, order_by, limit, model);
    step->setStepDescription(step_description);
    return step;
}

}
