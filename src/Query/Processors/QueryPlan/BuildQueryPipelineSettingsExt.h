#pragma once

#include <Interpreters/Context.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Query/Processors/QueryPlan/DistributedPipelineSettings.h>

namespace DB
{

class PlanSegment;
struct PlanSegmentExecutionInfo;
struct PlanSegmentPartitionSource;

struct BuildQueryPipelineSettingsExt : public BuildQueryPipelineSettings
{
    // BuildQueryPipelineSettingsExt() = default;
    // BuildQueryPipelineSettingsExt(const BuildQueryPipelineSettingsExt &) = default;
    // ~BuildQueryPipelineSettingsExt() = default;
    DistributedPipelineSettings distributed_settings;
    ContextPtr context;
    bool is_expand = false;
    std::unordered_map<UInt64, std::vector<PlanSegmentPartitionSource>> sources;

    static const BuildQueryPipelineSettingsExt & cast(const BuildQueryPipelineSettings & settings);
    static BuildQueryPipelineSettingsExt fromSettings(const Settings & from);
    static BuildQueryPipelineSettingsExt fromContext(ContextPtr from);
    static BuildQueryPipelineSettingsExt fromPlanSegment(PlanSegment * plan_segment, const PlanSegmentExecutionInfo & info, ContextPtr context,
        bool is_explain = false);
};


}
