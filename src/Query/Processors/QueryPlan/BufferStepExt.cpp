#include <Query/Processors/QueryPlan/BufferStepExt.h>
#include <Query/Processors/Transforms/BufferTransformExt.h>
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

}
