#pragma once

#include <Common/Logger.h>
#include <Core/SortDescription.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Transforms/finalizeChunk.h>
#include <Query/Processors/Transforms/AggregatingTransformExt.h>


namespace DB
{

class FinalizingSimpleTransformExt : public ISimpleTransform
{
public:
    FinalizingSimpleTransformExt(Block header, AggregatingTransformParamsExtPtr params_);

    void transform(Chunk & chunk) override;

    String getName() const override { return "FinalizingSimpleTransformExt"; }

private:
    AggregatingTransformParamsExtPtr params;
    ColumnsMask aggregates_mask;
};


}
