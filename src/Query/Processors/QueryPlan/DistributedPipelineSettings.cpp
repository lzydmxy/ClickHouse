#include <Query/Executor/PlanSegment.h>
#include <Query/Executor/PlanSegmentInstance.h>
#include <Query/Processors/QueryPlan/DistributedPipelineSettings.h>

namespace DB
{

DistributedPipelineSettings DistributedPipelineSettings::fromPlanSegment(PlanSegment * plan_segment, const PlanSegmentExecutionInfo & info)
{
    DistributedPipelineSettings settings;
    settings.is_distributed = true;
    settings.query_id = plan_segment->getQueryId();
    settings.plan_segment_id = plan_segment->getPlanSegmentId();
    settings.parallel_size = plan_segment->getParallelSize();
    settings.source_task_filter = info.source_task_filter;
    settings.coordinator_address = plan_segment->getCoordinatorAddress();
    settings.current_address = *(info.execution_address);
    return settings;
}

}
