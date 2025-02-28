#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class BufferStepExt : public ITransformingStep
{
public:
    friend class QueryPlanStepHelper;

    explicit BufferStepExt(const DataStream & input_stream_);

    String getName() const override { return "Buffer"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    void updateOutputStream() override;
};

}
