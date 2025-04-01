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
    //todo: now just a fake impl for build
    BuildQueryPipelineSettingsExt settings_ext;

    const ExpressionActionsSettings & getActionsSettings() const { return actions_settings; }
    static BuildQueryPipelineSettings fromContext(ContextPtr from);

    void initializeBuildQueryPipelineSettingsExt() const;
    const BuildQueryPipelineSettingsExt & getBuildQueryPipelineSettingsExt() const
    {
        //todo: now just a fake impl for build
        return settings_ext;
    }
};

}
