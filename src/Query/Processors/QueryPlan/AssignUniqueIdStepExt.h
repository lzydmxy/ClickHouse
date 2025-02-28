#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Query/Processors/QueryPlan/QueryPlanStepHelper.h>

namespace DB
{
class AssignUniqueIdStepExt : public ITransformingStep
{
public:
    explicit AssignUniqueIdStepExt(const DataStream & input_stream_, String unique_id_);

    String getName() const override { return "AssignUniqueId"; }
    // QueryPlanStepType getType() const  { return QueryPlanStepType::AssignUniqueIdStepExt; }

    void updateInputStreams(const DataStreams & input_streams_);
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    String getUniqueId() const { return unique_id; }
    friend class QueryPlanStepHelper;
private:
    String unique_id;
};

}
