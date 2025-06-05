#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>


namespace DB
{
class AssignUniqueIdStepExt : public ITransformingStep
{
public:
    explicit AssignUniqueIdStepExt(const DataStream & input_stream_, String unique_id_);

    String getName() const override { return "AssignUniqueIdExt"; }
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const;

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void toProto(Protos::AssignUniqueIdStepExt & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<AssignUniqueIdStepExt> fromProto(const Protos::AssignUniqueIdStepExt & proto, ContextPtr);

    String getUniqueId() const { return unique_id; }
    friend class QueryPlanStepHelper;
private:
    void updateOutputStream() override;

    String unique_id;
};

}
