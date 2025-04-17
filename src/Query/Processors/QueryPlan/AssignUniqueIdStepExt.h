#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>


namespace DB
{
class AssignUniqueIdStepExt : public ITransformingStep
{
public:
    explicit AssignUniqueIdStepExt(const DataStream & input_stream_, String unique_id_);

    String getName() const override { return "AssignUniqueIdStepExt"; }
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const;

    void updateInputStreams(const DataStreams & input_streams_);
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void toProto(Protos::AssignUniqueIdStep & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<AssignUniqueIdStepExt> fromProto(const Protos::AssignUniqueIdStep & proto, ContextPtr);

    String getUniqueId() const { return unique_id; }
    friend class QueryPlanStepHelper;
private:
    String unique_id;
};

}
