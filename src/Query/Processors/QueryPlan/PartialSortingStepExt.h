#pragma once

#include <Core/SortDescription.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

/// Sort separate chunks of data.
class PartialSortingStepExt : public ITransformingStep
{
public:
    explicit PartialSortingStepExt(const DataStream & input_stream, SortDescription sort_description_, UInt64 limit_, SizeLimits size_limits_ = {});

    String getName() const override { return "PartialSortingExt"; }

    const SortDescription & getSortDescription() const { return sort_description; }
    UInt64 getLimit() const { return limit; }
    const SizeLimits & getSizeLimits() const { return size_limits; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    /// Add limit or change it to lower value.
    void updateLimit(size_t limit_);

    void toProto(Protos::PartialSortingStepExt & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<PartialSortingStepExt> fromProto(const Protos::PartialSortingStepExt & proto, ContextPtr context);

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const;
    void setInputStreams(const DataStreams & input_streams_);

private:
    SortDescription sort_description;
    UInt64 limit;
    SizeLimits size_limits;
};

}
