#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class BufferStepExt : public ITransformingStep
{
public:
    explicit BufferStepExt(const DataStream & input_stream_);

    String getName() const override { return "BufferExt"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const;

    void toProto(Protos::BufferStepExt & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<BufferStepExt> fromProto(const Protos::BufferStepExt & proto, ContextPtr);

private:
    void updateOutputStream() override;
};

}
