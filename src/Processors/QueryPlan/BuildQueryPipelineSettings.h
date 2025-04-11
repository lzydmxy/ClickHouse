#pragma once

#include <IO/Progress.h>
#include <Interpreters/ExpressionActionsSettings.h>
#include <Query/Processors/QueryPlan/BuildQueryPipelineSettingsExt.h>

#include <cstddef>


namespace DB
{

struct Settings;
class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;

struct BuildQueryPipelineSettings
{
    ExpressionActionsSettings actions_settings;
    QueryStatusPtr process_list_element;
    ProgressCallback progress_callback = nullptr;

    const ExpressionActionsSettings & getActionsSettings() const { return actions_settings; }
    static BuildQueryPipelineSettings fromContext(ContextPtr from);

    /// only for jd optimizer
    BuildQueryPipelineSettingsExt settings_ext;

    void initBuildQueryPipelineSettingsExt(ContextPtr context)
    {
        settings_ext.fromContext(context);
    }

    void initBuildQueryPipelineSettingsExt(PlanSegment * plan_segment, const PlanSegmentExecutionInfo & info, ContextPtr context, bool is_explain)
    {
        settings_ext.fromPlanSegment(plan_segment, info, context, is_explain);
    }

    const BuildQueryPipelineSettingsExt & getBuildQueryPipelineSettingsExt() const
    {
        return settings_ext;
    }
};

}
