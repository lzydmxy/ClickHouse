#include <string>
#include <thread>
#include <base/scope_guard.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Context.h>
#include <Query/ProtosHelper/AddressInfo.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Poco/ConsoleChannel.h>
#include <Common/Stopwatch.h>
#include <gtest/gtest.h>
#include <Common/tests/gtest_global_context.h>
#include <Query/Common/OptimizerContext.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/Executor/PlanSegmentProcessList.h>
#include <Query/tests/gtest_common.h>

using namespace DB;

namespace UnitTest
{

Block createBlock()
{
    ColumnWithTypeAndName column;
    column.name = "RES";

    DataTypePtr type = DataTypeFactory::instance().get("UInt8");
    column.column = type->createColumnConst(1, Field(1));
    column.type = type;

    ColumnsWithTypeAndName columns;
    columns.push_back(column);

    return Block(columns);
}

QueryPlanExt generateEmptyPlan()
{
    QueryPlanExt plan;

    Block block = createBlock();
    auto step = std::make_unique<ReadNothingStep>(block);
    plan.addStep(std::move(step));

    return plan;
}

PlanSegmentProcessList::EntryPtr insertProcessList(PlanSegment & plan_segment, ContextMutablePtr context, bool force = false)
{
    auto & process_list = context->getPlanSegmentProcessList();
    auto plan_segment_process_entry = process_list.insertGroup(context, plan_segment.getPlanSegmentId(), force);
    process_list.insertProcessList(plan_segment_process_entry, plan_segment.getPlanSegmentId(), context, force);
    return plan_segment_process_entry;
}

TEST(PlanSegmentProcessListTest, InsertTest)
{
    const auto & context = getInitContext();
    // context->setTemporaryStoragePath("./tmp/", 1024);
    auto optimizer_context = context->getOptimizerContext();
    context->setProcessListEntry(nullptr);
    auto & client_info = context->getClientInfo();
    PlanSegment plan_segment = PlanSegment();
    plan_segment.setQueryId("PlanSegmentProcessList_test");
    plan_segment.setPlanSegmentId(0);
    plan_segment.setQueryPlan(generateEmptyPlan());

    client_info.current_query_id = plan_segment.getQueryId() + std::to_string(plan_segment.getPlanSegmentId());
    client_info.current_user = "test";
    client_info.initial_query_id = plan_segment.getQueryId();
    auto coordinator_address = std::make_shared<AddressInfo>("localhost", 8888, "test", "123456");
    optimizer_context->setCoordinatorAddress(coordinator_address);
    plan_segment.setCoordinatorAddress(*(coordinator_address.get()));
    insertProcessList(plan_segment, context);
}

// Cant replace process use same query id in ck 24.3
// TEST(PlanSegmentProcessListTest, InsertReplaceSuccessTest)
// {
//     const auto & context = getInitContext();
//     context->setSetting("replace_running_query", true);
//     //context->getSettings().replace_running_query = true;
//     // context->setTemporaryStoragePath("./tmp/", 1024);
//     auto optimizer_context = context->getOptimizerContext();
//     optimizer_context->setProcessListEntry(nullptr);
//     auto & client_info = context->getClientInfo();
//     PlanSegment plan_segment = PlanSegment();
//     plan_segment.setQueryId("PlanSegmentProcessList_test");
//     plan_segment.setPlanSegmentId(0);
//     plan_segment.setQueryPlan(generateEmptyPlan());

//     client_info.current_query_id = plan_segment.getQueryId() + std::to_string(plan_segment.getPlanSegmentId());
//     client_info.current_user = "test";
//     client_info.initial_query_id = plan_segment.getQueryId();
//     auto coordinator_address = std::make_shared<AddressInfo>("localhost", 8888, "test", "123456");
//     optimizer_context->setCoordinatorAddress(coordinator_address);
//     plan_segment.setCoordinatorAddress(*(coordinator_address.get()));
//     auto plan_segment_process_entry = insertProcessList(plan_segment, context);
//     auto async_func = [to_release_entry = std::move(plan_segment_process_entry)]() {
//         std::this_thread::sleep_for(std::chrono::milliseconds(1));
//         to_release_entry.get();
//     };
//     std::thread thread(std::move(async_func));
//     SCOPE_EXIT({
//         if (thread.joinable())
//             thread.join();
//     });
//     coordinator_address = std::make_shared<AddressInfo>("localhost", 8888, "test", "123456");;
//     optimizer_context->setCoordinatorAddress(coordinator_address);
//     plan_segment.setCoordinatorAddress(*(coordinator_address.get()));
//     insertProcessList(plan_segment, context, true);
// }

// TEST(PlanSegmentProcessListTest, InsertReplaceTimeoutTest)
// {
//     const auto & context = getContext().context;
//     // context->setTemporaryStoragePath("./tmp/", 1024);
//     auto optimizer_context = context->getOptimizerContext();
//     optimizer_context->setProcessListEntry(nullptr);
//     auto & client_info = context->getClientInfo();
//     PlanSegment plan_segment = PlanSegment();
//     plan_segment.setQueryId("PlanSegmentProcessList_test");
//     plan_segment.setPlanSegmentId(0);
//     plan_segment.setQueryPlan(generateEmptyPlan());

//     client_info.current_query_id = plan_segment.getQueryId() + std::to_string(plan_segment.getPlanSegmentId());
//     client_info.current_user = "test";
//     client_info.initial_query_id = plan_segment.getQueryId();
//     auto coordinator_address = std::make_shared<AddressInfo>("localhost", 8888, "test", "123456");
//     optimizer_context->setCoordinatorAddress(coordinator_address);
//     plan_segment.setCoordinatorAddress(*(coordinator_address.get()));
//     auto plan_segment_process_entry = insertProcessList(plan_segment, context);

//     auto async_func = [&, to_release_entry = std::move(plan_segment_process_entry)]() {
//         std::this_thread::sleep_for(
//             std::chrono::milliseconds(context->getSettingsRef().replace_running_query_max_wait_ms.totalMilliseconds() + 500));
//         to_release_entry.get();
//     };
//     std::thread thread(std::move(async_func));
//     SCOPE_EXIT({
//         if (thread.joinable())
//             thread.join();
//     });

//     coordinator_address = std::make_shared<AddressInfo>("localhost", 8888, "test", "123456");
//     optimizer_context->setCoordinatorAddress(coordinator_address);
//     plan_segment.setCoordinatorAddress(*(coordinator_address.get()));
//     ASSERT_THROW(insertProcessList(plan_segment, context, true), DB::Exception);
// }

}
