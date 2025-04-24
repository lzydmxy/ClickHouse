#include <Interpreters/JoinUtils.h>
#include <Query/Processors/QueryPlan/EnforceSingleRowStepExt.h>
#include <Query/Processors/Transforms/EnforceSingleRowTransformExt.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{
EnforceSingleRowStepExt::EnforceSingleRowStepExt(const DB::DataStream & input_stream_)
    : ITransformingStep(input_stream_, input_stream_.header, {})
{
    makeOutputNullable();
}

void EnforceSingleRowStepExt::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.resize(1);
    pipeline.addSimpleTransform([&](const Block & header) { return std::make_shared<EnforceSingleRowTransformExt>(header); });
}

void EnforceSingleRowStepExt::updateOutputStream()
{
    makeOutputNullable();
}

std::shared_ptr<IQueryPlanStep> EnforceSingleRowStepExt::copy(ContextPtr) const
{
    return std::make_unique<EnforceSingleRowStepExt>(input_streams[0]);
}

void EnforceSingleRowStepExt::makeOutputNullable()
{
    auto input_header = input_streams[0].header;
    ColumnsWithTypeAndName nullable_output_header;
    for (auto & input : input_header)
        if (!JoinCommon::canBecomeNullable(input.type))
            nullable_output_header.emplace_back(input.type, input.name);
        else
            nullable_output_header.emplace_back(JoinCommon::convertTypeToNullable(input.type), input.name);
    output_stream = DataStream{.header = {nullable_output_header}};
}

void EnforceSingleRowStepExt::toProto(Protos::EnforceSingleRowStepExt & proto, bool) const
{
    ProtosSerDerHelper::serializeToProtoBase(*this, *proto.mutable_query_plan_base());
}

std::shared_ptr<EnforceSingleRowStepExt> EnforceSingleRowStepExt::fromProto(const Protos::EnforceSingleRowStepExt & proto, ContextPtr)
{
    auto [step_description, base_input_stream] = ProtosSerDerHelper::deserializeFromProtoBase(proto.query_plan_base());
    auto step = std::make_shared<EnforceSingleRowStepExt>(base_input_stream);
    step->setStepDescription(step_description);
    return step;
}

}
