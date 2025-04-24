#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>
#include <QueryPipeline/SizeLimits.h>
#include <Disks/IVolume.h>

namespace DB
{

/// Merge streams of data into single sorted stream.
class MergingSortedStepExt : public ITransformingStep
{
public:
    explicit MergingSortedStepExt(
        const DataStream & input_stream,
        SortDescription sort_description_,
        size_t max_block_size_,
        UInt64 limit_ = 0);

    String getName() const override { return "MergingSortedExt"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    /// Add limit or change it to lower value.
    void updateLimit(size_t limit_);
    UInt64 getLimit() const { return limit; }
    size_t getMaxBlockSize() const { return max_block_size; }
    const SortDescription & getSortDescription() const { return sort_description; }

    void toProto(Protos::MergingSortedStepExt & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<MergingSortedStepExt> fromProto(const Protos::MergingSortedStepExt & proto, ContextPtr);

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const;
    void updateOutputStream() override;

private:
    SortDescription sort_description;
    size_t max_block_size;
    UInt64 limit;
};

}


