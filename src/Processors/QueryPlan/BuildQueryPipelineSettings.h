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
    BuildQueryPipelineSettingsExt build_pipeline_settings_ext;
    ExpressionActionsSettings actions_settings;
    QueryStatusPtr process_list_element;
    ProgressCallback progress_callback = nullptr;

    const BuildQueryPipelineSettingsExt & getBuildPipelineSettingsExt() const { return build_pipeline_settings_ext; }
    void initBuildPipelineSettingsExtFromContext(ContextPtr from) { build_pipeline_settings_ext.initFromContext(from); }
    void initBuildPipelineSettingsExtFromPlanSegment(
        PlanSegment * plan_segment, const PlanSegmentExecutionInfo & info, ContextPtr context, bool is_explain)
    {
        build_pipeline_settings_ext.initFromPlanSegment(plan_segment, info, context, is_explain);
    }

    const ExpressionActionsSettings & getActionsSettings() const { return actions_settings; }
    static BuildQueryPipelineSettings fromContext(ContextPtr from);
};

}
