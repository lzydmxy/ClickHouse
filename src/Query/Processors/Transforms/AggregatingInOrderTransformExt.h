#pragma once

#include <Common/Logger.h>
#include <Core/SortDescription.h>
#include <Interpreters/Aggregator.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/TotalsHavingTransform.h>
#include <Query/Processors/Transforms/AggregatingTransformExt.h>

namespace DB
{

class AggregatingInOrderTransformExt : public IProcessor
{

public:
    AggregatingInOrderTransformExt(Block header, AggregatingTransformParamsExtPtr params,
                                const SortDescriptionWithPositions & group_by_description, size_t res_block_size,
                                ManyAggregatedDataPtr many_data, size_t current_variant);

    AggregatingInOrderTransformExt(Block header, AggregatingTransformParamsExtPtr params,
                                const SortDescriptionWithPositions & group_by_description, size_t res_block_size);

    ~AggregatingInOrderTransformExt() override;

    String getName() const override { return "AggregatingInOrderTransformExt"; }

    Status prepare() override;

    void work() override;

    void consume(Chunk chunk);

private:
    void generate();

    size_t res_block_size;
    size_t cur_block_size = 0;

    MutableColumns res_key_columns;
    MutableColumns res_aggregate_columns;

    AggregatingTransformParamsExtPtr params;
    SortDescriptionWithPositions group_by_description;

    Aggregator::AggregateColumns aggregate_columns;

    ManyAggregatedDataPtr many_data;
    AggregatedDataVariants & variants;

    UInt64 src_rows = 0;
    UInt64 src_bytes = 0;
    UInt64 res_rows = 0;

    bool need_generate = false;
    bool block_end_reached = false;
    bool is_consume_started = false;
    bool is_consume_finished = false;

    Block res_header;
    Chunk current_chunk;
    Chunk to_push_chunk;

    LoggerPtr log = getLogger("AggregatingInOrderTransformExt");
};


class FinalizingSimpleTransformExt : public ISimpleTransform
{
public:
    FinalizingSimpleTransformExt(Block header, AggregatingTransformParamsExtPtr params_)
    : ISimpleTransform({std::move(header)}, {params_->getHeader()}, true)
    , params(params_)
    , aggregates_mask(getAggregatesMask(params->getHeader(), params->params.aggregates))
    {
    }

    void transform(Chunk & chunk) override
    {
        if (params->final)
            finalizeChunk(chunk, aggregates_mask);
        else if (!chunk.getChunkInfo())
        {
            auto info = std::make_shared<AggregatedChunkInfo>();
            chunk.setChunkInfo(std::move(info));
        }
    }

    String getName() const override { return "FinalizingSimpleTransformExt"; }

private:
    AggregatingTransformParamsExtPtr params;
    ColumnsMask aggregates_mask;
};


}
