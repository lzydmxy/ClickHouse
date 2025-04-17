#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{
class EnforceSingleRowStepExt : public ITransformingStep
{
public:
    explicit EnforceSingleRowStepExt(const DataStream & input_stream_);

    String getName() const override { return "EnforceSingleRowStepExt"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const;
    /// make output columns nullable, we should generate a null output value when subquery return empty results
    void makeOutputNullable();

    void toProto(Protos::EnforceSingleRowStep & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<EnforceSingleRowStepExt> fromProto(const Protos::EnforceSingleRowStep & proto, ContextPtr);

private:
    void updateOutputStream() override;
};

}
