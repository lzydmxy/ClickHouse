#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class BufferStepExt : public ITransformingStep
{
public:
    explicit BufferStepExt(const DataStream & input_stream_);

    String getName() const override { return "BufferStepExt"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const;

private:
    void updateOutputStream() override;
};

}
