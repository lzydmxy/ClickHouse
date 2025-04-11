#pragma once

#include <Query/Processors/QueryPlan/DistributedPipelineSettings.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>

namespace DB
{

struct BuildQueryPipelineSettingsHelper
{
    static BuildQueryPipelineSettings fromContextExt(ContextPtr from)
    {
        auto settings = BuildQueryPipelineSettings::fromContext(from);
        settings.initBuildQueryPipelineSettingsExt(from);
        return settings;
    }

    static BuildQueryPipelineSettings fromPlanSegmentExt(
        PlanSegment * plan_segment, const PlanSegmentExecutionInfo & info, ContextPtr context, bool is_explain)
    {
        auto settings = BuildQueryPipelineSettings::fromContext(context);
        settings.initBuildQueryPipelineSettingsExt(plan_segment, info, context, is_explain);
        return settings;
    }
};

}


