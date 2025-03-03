#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{
class EnforceSingleRowStepExt : public ITransformingStep
{
public:
    explicit EnforceSingleRowStepExt(const DataStream & input_stream_);

    String getName() const override { return "EnforceSingleRowExt"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const;
    /// make output columns nullable, we should generate a null output value when subquery return empty results
    void makeOutputNullable();

private:
    void updateOutputStream() override;
};

}
