#pragma once

#include <Query/Processors/QueryPlan/DistributedPipelineSettings.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>

namespace DB
{

class BuildQueryPipelineSettingsHelper
{
    static BuildQueryPipelineSettings fromContextExt(ContextPtr from)
    {
        auto settings = BuildQueryPipelineSettings::fromContext(from);
        auto build_pipeline_settings_ext = settings.getBuildQueryPipelineSettingsExt();
        build_pipeline_settings_ext.fromContext(from);
        return settings;
    }

    static BuildQueryPipelineSettings fromPlanSegmentExt(
        PlanSegment * plan_segment, const PlanSegmentExecutionInfo & info, ContextPtr context, bool is_explain)
    {
        auto settings = BuildQueryPipelineSettings::fromContext(context);
        auto build_pipeline_settings_ext = settings.getBuildQueryPipelineSettingsExt();
        build_pipeline_settings_ext.fromPlanSegment(plan_segment, info, context, is_explain);
        return settings;
    }
};

}


