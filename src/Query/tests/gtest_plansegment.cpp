#include <gtest/gtest.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/tests/gtest_common.h>
#include <Query/tests/gtest_protobuf_common.h>

using namespace DB;

namespace UnitTest
{

PlanSegmentPtr createPlanSegment()
{
    PlanSegmentPtr plan_segment = std::make_unique<PlanSegment>();

    PlanSegmentInputPtr left = std::make_shared<PlanSegmentInput>(RIPlanSegment::EXCHANGE);
    PlanSegmentInputPtr right = std::make_shared<PlanSegmentInput>(RIPlanSegment::EXCHANGE);
    PlanSegmentOutputPtr output = std::make_shared<PlanSegmentOutput>(RIPlanSegment::OUTPUT);

    plan_segment->appendPlanSegmentInput(left);
    plan_segment->appendPlanSegmentInput(right);
    plan_segment->appendPlanSegmentOutput(output);

    auto step = std::make_unique<ReadNothingStep>(Block{});
    // QueryPlan::Node remote_node{.step = std::move(step), .children = {}};
    // query_plan.addRoot(std::move(remote_node));
    QueryPlanExt query_plan;
    query_plan.addStep(std::move(step));
    plan_segment->setQueryPlan(std::move(query_plan));

    return plan_segment;
}
 
TEST(PlanSegmentTest, PlanSegmentSerDer)
{
    auto context = getInitContext();
    PlanSegmentPtr plan_segment1 = createPlanSegment();
    /**
     * serialize to buffer
     */
    // WriteBufferFromOwnString write_buffer;
    // plan_segment->serialize(write_buffer);
    /**
     * deserialize from buffer
     */
    // ReadBufferFromString read_buffer(write_buffer.str());
    // PlanSegmentPtr new_plan_segment = std::make_unique<PlanSegment>();
    // new_plan_segment->deserialize(read_buffer, ContextMutablePtr());
    RPlanSegment pb1, pb2;
    plan_segment1->toProto(pb1);

    PlanSegmentPtr plan_segment2 = std::make_unique<PlanSegment>();
    plan_segment2->fromProto(pb1, context);
    plan_segment2->toProto(pb2);
    ProtobufTest::compareProto(pb1, pb2);
}

}
