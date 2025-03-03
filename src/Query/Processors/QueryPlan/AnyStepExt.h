#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

using GroupId = UInt32;

class AnyStepExt : public IQueryPlanStep
{
public:
    AnyStepExt(DataStream output, GroupId group_id_) : group_id(group_id_) { output_stream = output; }

    String getName() const override { return "AnyStepExt"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    GroupId getGroupId() const { return group_id; }

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr context) const;

private:
    GroupId group_id;
};

}
