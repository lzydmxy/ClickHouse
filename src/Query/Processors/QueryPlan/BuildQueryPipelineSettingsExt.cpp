#include <Query/Executor/PlanSegmentInstance.h>
#include <Query/Processors/QueryPlan/BuildQueryPipelineSettingsExt.h>

namespace DB
{

const BuildQueryPipelineSettingsExt & BuildQueryPipelineSettingsExt::cast(const BuildQueryPipelineSettings & settings)
{
    //return *(static_cast<const BuildQueryPipelineSettingsExt1 *>(&settings));
    return static_cast<const BuildQueryPipelineSettingsExt &>(settings);
}

BuildQueryPipelineSettingsExt BuildQueryPipelineSettingsExt::fromSettings(const Settings & from)
{
    BuildQueryPipelineSettingsExt settings;
    settings.actions_settings = ExpressionActionsSettings::fromSettings(from, CompileExpressions::yes);
    //In all of its usage, there is no use of an uninitialized variable
    //settings.distributed_settings.coordinator_address.port will be initialized when it's used
    //coverity[uninit_use]
    return settings;
}

BuildQueryPipelineSettingsExt BuildQueryPipelineSettingsExt::fromContext(ContextPtr from)
{
    auto settings = fromSettings(from->getSettingsRef());
    settings.context = from;
    return settings;
}

BuildQueryPipelineSettingsExt BuildQueryPipelineSettingsExt::fromPlanSegment(PlanSegment * plan_segment, const PlanSegmentExecutionInfo & info, ContextPtr context, bool is_explain)
{
    auto settings = fromContext(context);
    settings.distributed_settings = DistributedPipelineSettings::fromPlanSegment(plan_segment, info);
    settings.distributed_settings.is_explain = is_explain;
    settings.context = context;
    settings.sources = info.sources;
    return settings;
}

}
