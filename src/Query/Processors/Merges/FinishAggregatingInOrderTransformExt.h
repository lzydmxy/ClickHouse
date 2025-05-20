#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Query/Processors/Merges/Algorithms/FinishAggregatingInOrderAlgorithmExt.h>

namespace DB
{

class ColumnAggregateFunction;

/// Implementation of IMergingTransform via FinishAggregatingInOrderAlgorithm.
class FinishAggregatingInOrderTransformExt final : public IMergingTransform<FinishAggregatingInOrderAlgorithmExt>
{
public:
    FinishAggregatingInOrderTransformExt(
        const Block & header,
        size_t num_inputs,
        AggregatingTransformParamsExtPtr params,
        SortDescriptionWithPositions description,
        size_t max_block_size)
        : IMergingTransform(
            num_inputs, header, header, true, 0, false,
            header,
            num_inputs,
            params,
            std::move(description),
            max_block_size)
    {
    }

    String getName() const override { return "FinishAggregatingInOrderTransformExt"; }
};

}
