#include <Query/Executor/PlanSegmentInstance.h>
#include <Query/Processors/QueryPlan/BuildQueryPipelineSettingsExt.h>

namespace DB
{

void BuildQueryPipelineSettingsExt::fromContext(ContextPtr from)
{
    this->context = from;
}

void BuildQueryPipelineSettingsExt::fromPlanSegment(
    PlanSegment * plan_segment, const PlanSegmentExecutionInfo & info, ContextPtr context, bool is_explain)
{
    this->distributed_settings = DistributedPipelineSettings::fromPlanSegment(plan_segment, info);
    this->distributed_settings.is_explain = is_explain;
    this->context = context;
    this->sources = info.sources;
}

}
