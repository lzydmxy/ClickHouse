#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Query/Processors/QueryPlan/TopNModel.h>

namespace DB
{

/// Sorts stream of data. See MergeSortingTransform.
class PartitionTopNStepExt : public ITransformingStep
{
public:
    friend class QueryPlanStepHelper;

    explicit PartitionTopNStepExt(
        const DataStream & input_stream_, const Names & partition_, const Names & order_by_, UInt64 limit_, TopNModel model_);

    String getName() const override { return "PartitionTopN"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const Names & getPartition() const { return partition; }
    const Names & getOrderBy() const { return order_by; }
    UInt64 getLimit() const { return limit; }
    TopNModel getModel() const { return model; }

private:
    Names partition;
    Names order_by;
    UInt64 limit;
    TopNModel model;

    void updateOutputStream() override;
};

}
