#include <Interpreters/Context.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/Optimizer/PlanNodeSearcher.h>
#include <Query/Optimizer/PlanOptimizer.h>
#include <Query/Optimizer/Rewriter/ColumnPruning.h>
#include <Query/Optimizer/tests/test_config.h>
#include <Query/Processors/QueryPlan/QueryPlanExt.h>
#include <Query/Core/BlockHelper.h>
#include <gtest/gtest.h>
#include "gtest_base_plan_test.h"

#include <boost/asio.hpp>

#include <algorithm>
#include <filesystem>
#include <memory>

using namespace DB;

TEST(OptimizerColumnPruning, JoinOutputsNotRequired)
{
    BasePlanTest test;
    auto context = test.createQueryContext();
    context->setSetting("joined_subquery_requires_alias", String("0"));
    auto plan = test.plan("select a from (select 1 a), (select b from (select 2 b), (select c from (select 3 c), (select 4 d)));", context);
    PlanOptimizer::optimize(*plan, context, Rewriters{{std::make_shared<ColumnPruning>()}});

    auto joins = PlanNodeSearcher::searchFrom(plan->getPlanNode())
                     .where([](auto & node) { return getQueryPlanStepType(node.getStep()) == QueryPlanStepType::JoinStepExt; })
                     .findAll();

    for (const auto & join : joins)
    {
        auto outputs = BlockHelper::getNamesToTypes(join->getStep()->getOutputStream().header);
        auto left_inputs = BlockHelper::getNamesToTypes(join->getStep()->getInputStreams()[0].header);
        auto right_inputs = BlockHelper::getNamesToTypes(join->getStep()->getInputStreams()[1].header);

        EXPECT_TRUE(std::none_of(left_inputs.begin(), left_inputs.end(), [&](const auto & left) { return right_inputs.count(left.first); }))
            << "no duplicate symbols in left and right";

        EXPECT_TRUE(std::all_of(outputs.begin(), outputs.end(), [&](const auto & o) {
            return left_inputs.count(o.first) || right_inputs.count(o.first);
        })) << "output symbol must exist in left or right";
    }
}

TEST(OptimizerColumnPruning, JoinOutputsNotRequired2)
{
    BasePlanTest test;
    auto context = test.createQueryContext();
    context->setSetting("joined_subquery_requires_alias", String("0"));
    auto plan = test.plan("select 'a' from (select 1 a), (select 'b' from (select 2 b), (select 'c' from (select 3 c), (select 4 d)));", context);
    PlanOptimizer::optimize(*plan, context, Rewriters{{std::make_shared<ColumnPruning>()}});

    auto joins = PlanNodeSearcher::searchFrom(plan->getPlanNode())
                     .where([](auto & node) { return getQueryPlanStepType(node.getStep()) == QueryPlanStepType::JoinStepExt; })
                     .findAll();

    for (const auto & join : joins)
    {
        auto outputs = BlockHelper::getNamesToTypes(join->getStep()->getOutputStream().header);
        auto left_inputs = BlockHelper::getNamesToTypes(join->getStep()->getInputStreams()[0].header);
        auto right_inputs = BlockHelper::getNamesToTypes(join->getStep()->getInputStreams()[1].header);

        EXPECT_TRUE(std::none_of(left_inputs.begin(), left_inputs.end(), [&](const auto & left) { return right_inputs.count(left.first); }))
            << "no duplicate symbols in left and right";

        EXPECT_TRUE(std::all_of(outputs.begin(), outputs.end(), [&](const auto & o) {
            return left_inputs.count(o.first) || right_inputs.count(o.first);
        })) << "output symbol must exist in left or right";
    }
}

