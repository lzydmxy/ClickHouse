#pragma once

#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>

namespace DB
{

/// Finish sorting of pre-sorted data. See FinishSortingTransform.
class FinishSortingStepExt : public ITransformingStep
{
public:
    FinishSortingStepExt(
        const DataStream & input_stream_,
        SortDescription prefix_description_,
        SortDescription result_description_,
        size_t max_block_size,
        UInt64 limit);

    String getName() const override { return "FinishSortingExt"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    SortDescription getPrefixDescription() const { return prefix_description; }
    SortDescription getResultDescription() const { return result_description; }
    size_t getMaxBlockSize() const { return max_block_size; }
    UInt64 getLimit() const { return limit; }

    /// Add limit or change it to lower value.
    void updateLimit(size_t limit_);

    void toProto(Protos::FinishSortingStepExt & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<FinishSortingStepExt> fromProto(const Protos::FinishSortingStepExt & proto, ContextPtr);
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const;
    void setInputStreams(const DataStreams & input_streams_);

private:
    SortDescription prefix_description;
    SortDescription result_description;
    size_t max_block_size;
    UInt64 limit;
};

}
