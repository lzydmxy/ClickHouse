#include <Query/Processors/QueryPlan/BufferStepExt.h>
#include <Query/Processors/Transforms/BufferTransformExt.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

BufferStepExt::BufferStepExt(const DataStream & input_stream_) : ITransformingStep(input_stream_, input_stream_.header, Traits{})
{
}

void BufferStepExt::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform(
        [&](const Block & header)
        {
            auto transform = std::make_shared<BufferTransformExt>(header);
            return transform;
        });
}

void BufferStepExt::updateOutputStream()
{
    output_stream->header = input_streams[0].header;
}

std::shared_ptr<IQueryPlanStep> BufferStepExt::copy(ContextPtr) const
{
    return std::make_shared<BufferStepExt>(input_streams[0]);
}

void BufferStepExt::toProto(Protos::BufferStep & proto, bool for_hash_equals = false) const
{
    (void)for_hash_equals;
    ProtosSerDerHelper::serializeToProtoBase(*this, *proto.mutable_query_plan_base());
}

std::shared_ptr<BufferStepExt> BufferStepExt::fromProto(const Protos::BufferStep & proto, ContextPtr)
{
    auto [step_description, base_input_stream] = ProtosSerDerHelper::deserializeFromProtoBase(proto.query_plan_base());
    auto step = std::make_shared<BufferStepExt>(base_input_stream);
    step->setStepDescription(step_description);
    return step;
}

}
