#include <gtest/gtest.h>
#include <Query/Processors/QueryPlan/BuildQueryPipelineSettingsExt.h>
#include <Query/Executor/PlanSegment.h>

using namespace DB;


TEST(BuildQueryPipelineSettingsExtTest, fromPlanSegmentTest)
{
    PlanSegment plan_segment(10, "query_id_01", "cluster_01");
    plan_segment.setParallelSize(10);
    AddressInfo coordinator_address = {"1.1.1.1", 9600, "user", "password"};
    plan_segment.setCoordinatorAddress(coordinator_address);

    auto global_context = Context::getGlobalContextInstance();

    PlanSegmentExecutionInfo info;
    AddressInfoPtr current_address = std::make_shared<AddressInfo>("1.1.1.2", 9600, "user", "password");
    info.execution_address = current_address;
    SourceTaskFilter source_task_filter{.index=1, .count=1, .buckets={1}};
    info.source_task_filter = source_task_filter;

    auto pipeline_settings = BuildQueryPipelineSettingsExt::fromPlanSegment(&plan_segment, info, global_context, true);


    EXPECT_EQ(pipeline_settings.distributed_settings.is_distributed, true);
    EXPECT_EQ(pipeline_settings.distributed_settings.query_id, "query_id_01");
    EXPECT_EQ(pipeline_settings.distributed_settings.plan_segment_id, 10);
    EXPECT_EQ(pipeline_settings.distributed_settings.parallel_size, 10);
    EXPECT_EQ(pipeline_settings.distributed_settings.source_task_filter.index, source_task_filter.index);
    EXPECT_EQ(pipeline_settings.distributed_settings.source_task_filter.count, source_task_filter.count);
    EXPECT_EQ(pipeline_settings.distributed_settings.source_task_filter.buckets, source_task_filter.buckets);
    EXPECT_EQ(pipeline_settings.distributed_settings.is_explain, true);
    EXPECT_EQ(pipeline_settings.distributed_settings.coordinator_address, coordinator_address);
    EXPECT_EQ(pipeline_settings.distributed_settings.current_address, *current_address);
}
