#include <chrono>
#include <Core/Types.h>
#include <DataTypes/DataTypeFactory.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Query/ProtosHelper/HostWithPorts.h>
#include <Query/ProtosHelper/AddressInfo.h>
#include <Query/ProtosHelper/SourceTask.h>
#include <Query/Executor/DAGGraph.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/Executor/sendPlanSegment.h>
#include <Query/Executor/NodeSelector.h>
#include <Query/Executor/RuntimeSegmentsStatus.h>
#include <Query/Executor/MPPScheduler.h>
#include <Query/tests/gtest_exchange_helper.h>
#include <Query/tests/gtest_common.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <Common/tests/gtest_global_context.h>


using namespace DB;

namespace UnitTest
{

QueryPlanExt createEmptyPlan()
{
    ColumnWithTypeAndName column;
    column.name = "RES";

    DataTypePtr type = DataTypeFactory::instance().get("UInt8");
    column.column = type->createColumnConst(1, Field(1));
    column.type = type;

    ColumnsWithTypeAndName columns;
    columns.push_back(column);
    Block block = Block(columns);

    QueryPlanExt plan;

    auto step = std::make_unique<ReadNothingStep>(block);
    plan.addStep(std::move(step));

    return plan;
}

struct SchedulerTestContext
{
    String query_id;
    std::string cluster_name;
    DAGGraphPtr dag_graph_ptr;
    bool batch_schedule;
    ContextMutablePtr query_context;
    std::vector<std::shared_ptr<PlanSegment>> segments;
    ClusterNodes cluster_nodes;
};

SchedulerTestContext createSchedulerTestContext(size_t parallel_size, const std::unordered_map<std::string, Field> & settings)
{
    SchedulerTestContext result;
    result.query_context = createQueryContext("q1", settings);
    ContextPtr context(result.query_context);

    /// prepare plan segment
    result.segments = {std::make_shared<PlanSegment>(2, "q1", "c1")
        , std::make_shared<PlanSegment>(0, "q1", "c2")};

    result.segments[0]->setParallelSize(parallel_size);
    result.segments[1]->setParallelSize(parallel_size);
    result.segments[0]->setQueryPlan(createEmptyPlan());
    result.segments[1]->setQueryPlan(createEmptyPlan());
    // result.segments[2]->setParallelSize(parallel_size);
    Block header;
    std::vector<PlanSegmentInputPtr> segment_inputs
        = {std::make_shared<PlanSegmentInput>(header, RIPlanSegment::SOURCE),
           std::make_shared<PlanSegmentInput>(header, RIPlanSegment::EXCHANGE)};
    segment_inputs[1]->setPlanSegmentId(2);
    segment_inputs[1]->setExchangeId(1);
    result.segments[0]->appendPlanSegmentInput(segment_inputs[0]);
    result.segments[1]->appendPlanSegmentInput(segment_inputs[1]);

    PlanSegmentOutputPtr segment_output1 = std::make_shared<PlanSegmentOutput>(header, RIPlanSegment::OUTPUT);
    segment_output1->setParallelSize(parallel_size);
    result.segments[0]->appendPlanSegmentOutput(segment_output1);
    result.segments[1]->appendPlanSegmentOutput(segment_output1);

    /// initialize dag_graph_ptr
    result.dag_graph_ptr = std::make_shared<DAGGraph>();
    result.dag_graph_ptr->setContext(context);
    result.dag_graph_ptr->id_to_segment[0] = result.segments[1].get();
    result.dag_graph_ptr->id_to_segment[2] = result.segments[0].get();
    result.dag_graph_ptr->leaf_segments = {2};
    result.dag_graph_ptr->table_scan_or_value_segments = {2};
    result.dag_graph_ptr->exchanges[0] = {segment_inputs[1], segment_output1};
    result.dag_graph_ptr->final = 0;

    prepareQueryCommonBuf(result.dag_graph_ptr->query_common_buf, *(result.segments[0]), context);

    result.cluster_nodes.all_workers
        = {WorkerNode(AddressInfo("10.10.10.10", 9010, "", "", 9011), NodeType::Remote),
           WorkerNode(AddressInfo("10.10.10.11", 9010, "", "", 9011), NodeType::Remote)};
    result.cluster_nodes.rank_worker_ids = {1, 2};
    result.cluster_nodes.all_hosts = {HostWithPorts{"10.10.10.10", 9010}, HostWithPorts{"10.10.10.11", 9010}};
    result.cluster_nodes.cluster_name = "cluster1";
    result.cluster_name = "cluster1";

    return result;
}

TEST(SchedulerTest, MPPSchedule)
{
    size_t parallel_size = 2;
    std::unordered_map<std::string, Field> settings{{"bsp_mode", 0}
        , {"distributed_max_parallel_size", parallel_size}};

    auto scheduler_context = createSchedulerTestContext(parallel_size, settings);
    scheduler_context.query_context->getOptimizerContext()->setQueryMaxExecutionTime(2000);

    MPPScheduler scheduler(scheduler_context.query_id, scheduler_context.cluster_nodes, scheduler_context.query_context,
        scheduler_context.dag_graph_ptr,
        scheduler_context.query_context->getOptimizerContext()->getSettingsRef().enable_batch_send_plan_segment,
        true);

    auto execution_info = scheduler.schedule();

    ASSERT_EQ(0, execution_info.parallel_id);
}

}
