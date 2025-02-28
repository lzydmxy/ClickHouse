#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{
class EnforceSingleRowStepExt : public ITransformingStep
{
public:
    friend class QueryPlanStepHelper;

    explicit EnforceSingleRowStepExt(const DataStream & input_stream_);

    String getName() const override { return "EnforceSingleRow"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    /// make output columns nullable, we should generate a null output value when subquery return empty results
    void makeOutputNullable();

private:
    void updateOutputStream() override;
};

}
