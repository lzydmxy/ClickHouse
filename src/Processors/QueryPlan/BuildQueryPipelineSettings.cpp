#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Core/Settings.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Context.h>

namespace DB
{

const BuildQueryPipelineSettingsExt & BuildQueryPipelineSettings::getBuildPipelineSettingsExt() const
{
    if (!build_pipeline_settings_ext.has_value())
        throw Exception(ErrorCodes::BAD_GET, "Bad get, build_pipeline_settings_ext is not set");
    return build_pipeline_settings_ext.value();
}

BuildQueryPipelineSettings BuildQueryPipelineSettings::fromContextExt(ContextPtr from)
{
    auto settings = fromContext(from);
    settings.build_pipeline_settings_ext->fromContext(from);
    return settings;
}

BuildQueryPipelineSettings BuildQueryPipelineSettings::fromPlanSegmentExt(
    PlanSegment * plan_segment, const PlanSegmentExecutionInfo & info, ContextPtr context, bool is_explain)
{
    auto settings = fromContext(context);
    settings.build_pipeline_settings_ext->fromPlanSegment(plan_segment, info, context, is_explain);
    return settings;
}

BuildQueryPipelineSettings BuildQueryPipelineSettings::fromContext(ContextPtr from)
{
    BuildQueryPipelineSettings settings;
    settings.actions_settings = ExpressionActionsSettings::fromSettings(from->getSettingsRef(), CompileExpressions::yes);
    settings.process_list_element = from->getProcessListElement();
    settings.progress_callback = from->getProgressCallback();
    return settings;
}

}
