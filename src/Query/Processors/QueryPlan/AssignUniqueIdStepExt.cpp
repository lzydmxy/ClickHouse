#include <Query/Processors/QueryPlan/AssignUniqueIdStepExt.h>

#include <DataTypes/DataTypesNumber.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Query/Processors/Transforms/AssignUniqueIdTransformExt.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>

namespace DB
{
    AssignUniqueIdStepExt::AssignUniqueIdStepExt(const DataStream & input_stream_, String unique_id_)
    : ITransformingStep(input_stream_, AssignUniqueIdTransformExt::transformHeader(input_stream_.header, unique_id_), {})
    , unique_id(std::move(unique_id_))
{
}

void AssignUniqueIdStepExt::updateOutputStream()
{
    output_stream = input_streams[0];
    output_stream->header.insert(ColumnWithTypeAndName{std::make_shared<DataTypeUInt64>(), unique_id});
}

void AssignUniqueIdStepExt::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&](const Block & header) { return std::make_shared<AssignUniqueIdTransformExt>(header, unique_id); });
}

std::shared_ptr<IQueryPlanStep> AssignUniqueIdStepExt::copy(ContextPtr) const
{
    return std::make_unique<AssignUniqueIdStepExt>(input_streams[0], unique_id);
}

std::shared_ptr<AssignUniqueIdStepExt> AssignUniqueIdStepExt::fromProto(const Protos::AssignUniqueIdStepExt & proto, ContextPtr)
{
    auto [step_description, base_input_stream] = ProtosSerDerHelper::deserializeFromProtoBase(proto.query_plan_base());
    auto unique_id = proto.unique_id();
    auto step = std::make_shared<AssignUniqueIdStepExt>(base_input_stream, unique_id);
    step->setStepDescription(step_description);
    return step;
}

void AssignUniqueIdStepExt::toProto(Protos::AssignUniqueIdStepExt & proto, bool) const
{
    ProtosSerDerHelper::serializeToProtoBase(*this,*proto.mutable_query_plan_base());
    proto.set_unique_id(unique_id);
}

}
