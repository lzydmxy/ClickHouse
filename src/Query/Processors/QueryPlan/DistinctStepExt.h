#pragma once

#include <Processors/QueryPlan/DistinctStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Core/Names.h>
/*
#include <QueryPlan/ITransformingStep.h>
#include <DataStreams/SizeLimits.h>
*/


namespace DB
{

class DistinctStepExt : public DistinctStep
{
public:
DistinctStepExt(
            const DataStream & input_stream_,
            const SizeLimits & set_size_limits_,
            UInt64 limit_hint_,
            const Names & columns_,
            bool pre_distinct_,
            bool optimize_distinct_in_order_,
            bool can_to_agg_); /// If is enabled, execute distinct for separate streams. Otherwise, merge streams.

    String getName() const override { return "DistinctStepExt"; }
    const SizeLimits & getSetSizeLimits() const { return set_size_limits; }
    void setLimitHint(UInt64 limit_hint_) { limit_hint = limit_hint_; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const;

public:
    bool can_to_agg;
};

}
