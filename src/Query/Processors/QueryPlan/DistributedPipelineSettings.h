#pragma once

#include <Query/ProtosHelper/AddressInfo.h>
#include <Query/ProtosHelper/SourceTask.h>

namespace DB
{

class PlanSegment;
struct PlanSegmentExecutionInfo;

struct DistributedPipelineSettings
{
    bool is_distributed = false;
    String query_id;
    size_t plan_segment_id = 0;
    size_t parallel_size = 1;
    SourceTaskFilter source_task_filter;
    bool is_explain = false; // explain pipeline sql set is_explain to true
    AddressInfo coordinator_address;
    AddressInfo current_address;

    static DistributedPipelineSettings fromPlanSegment(PlanSegment * plan_segment, const PlanSegmentExecutionInfo & info);
};

}
