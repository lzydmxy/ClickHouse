#pragma once

#include <Interpreters/Context.h>
#include <Query/Processors/QueryPlan/DistributedPipelineSettings.h>

namespace DB
{

class PlanSegment;
struct PlanSegmentExecutionInfo;
struct PlanSegmentPartitionSource;

struct BuildQueryPipelineSettingsExt
{
    DistributedPipelineSettings distributed_settings;
    ContextPtr context;
    bool is_expand = false;
    std::unordered_map<UInt64, std::vector<PlanSegmentPartitionSource>> sources;

    void fromContext(ContextPtr from);
    void fromPlanSegment(PlanSegment * plan_segment, const PlanSegmentExecutionInfo & info, ContextPtr context, bool is_explain = false);
};

}
