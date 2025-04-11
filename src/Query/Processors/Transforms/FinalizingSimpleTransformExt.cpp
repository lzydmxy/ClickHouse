#include <Query/Processors/Transforms/FinalizingSimpleTransformExt.h>

namespace DB
{

FinalizingSimpleTransformExt::FinalizingSimpleTransformExt(Block header, AggregatingTransformParamsExtPtr params_)
    : ISimpleTransform({std::move(header)}, {params_->getHeader()}, true)
    , params(params_)
    , aggregates_mask(getAggregatesMask(params->getHeader(), params->params.aggregates))
{
}

void FinalizingSimpleTransformExt::transform(Chunk & chunk)
{
    if (params->final)
        finalizeChunk(chunk, aggregates_mask);
    else if (!chunk.getChunkInfo())
    {
        auto info = std::make_shared<AggregatedChunkInfo>();
        chunk.setChunkInfo(std::move(info));
    }
}


}
