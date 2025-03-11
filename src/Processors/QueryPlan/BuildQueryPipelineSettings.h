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
    std::optional<BuildQueryPipelineSettingsExt> build_pipeline_settings_ext;
    ExpressionActionsSettings actions_settings;
    QueryStatusPtr process_list_element;
    ProgressCallback progress_callback = nullptr;

    const BuildQueryPipelineSettingsExt & getBuildPipelineSettingsExt() const;
    static BuildQueryPipelineSettings fromContextExt(ContextPtr from);
    static BuildQueryPipelineSettings
    fromPlanSegmentExt(PlanSegment * plan_segment, const PlanSegmentExecutionInfo & info, ContextPtr context, bool is_explain);

    const ExpressionActionsSettings & getActionsSettings() const { return actions_settings; }
    static BuildQueryPipelineSettings fromContext(ContextPtr from);
};

}
