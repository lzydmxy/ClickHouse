#include <Query/Optimizer/tests/gtest_base_unittest_tester.h>
#include <Query/Processors/QueryPlan/PlanPrinter.h>
#include "Interpreters/Context.h"

#include <Interpreters/Context_fwd.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/Optimizer/tests/test_config.h>
#include <Query/Processors/QueryPlan/QueryPlanExt.h>

#include <Interpreters/InterpreterDropQuery.h>
#include <Query/Interpreters/InterpreterSelectQueryUseOptimizer.h>
#include <Interpreters/InterpreterCreateQuery.h>
// #include <Interpreters/InterpreterShowStatsQuery.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Interpreters/SelectIntersectExceptQueryVisitor.h>
#include <Interpreters/executeQuery.h>
#include <Query/Optimizer/CardinalityEstimate/CardinalityEstimator.h>

#include <Query/Optimizer/Dump/PlanReproducer.h>
#include <Query/Optimizer/PlanOptimizer.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Query/Statistics/CacheManager.h>
#include <Query/Statistics/CatalogAdaptor.h>
#include <Storages/System/attachSystemTables.h>
#include <Poco/NumberParser.h>
#include <Poco/StringTokenizer.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/Exception.h>
#include <Query/Optimizer/tests/gtest_storage_mock_distirbuted.h>

namespace DB
{

void OptimizerTester::registerTable(const String & DDL)
{
    QueryStatistics::CacheManager::initialize(100000, std::chrono::seconds(1800));

    Poco::StringTokenizer tokenizer(DDL, ";", Poco::StringTokenizer::Options::TOK_IGNORE_EMPTY | Poco::StringTokenizer::Options::TOK_TRIM);

    for (auto ddl : tokenizer)
    {
        if (ddl.empty())
            continue;
        ASTPtr ast = BasePlanTest::parse(ddl, session_context);
        if (auto * create = ast->as<ASTCreateQuery>())
        {
            if (create->storage)
            {
                auto * storage = create->storage->as<ASTStorage>();
                storage->engine->name = StorageMockDistributed::ENGINE_NAME;
            }
        }

        ThreadStatus thread_status;
        // thread_status.attachQueryContext(session_context); // todo wujianchao add method attachQueryContext to ThreadStatus
        InterpreterCreateQuery create_interpreter(ast, session_context);
        create_interpreter.execute();
    }
}


void OptimizerTester::assertMatch(const QueryPlanExtPtr & plan, PlanNodePtr expected_plan_node, CTEInfo expected_cte_info, QueryPlanSettings explain_settings) const
{
    QueryPlanExt expected_plan = QueryPlanExt(expected_plan_node, expected_cte_info, session_context->getOptimizerContext()->getPlanNodeIdAllocator());
    if (PlanPrinter::textLogicalPlan(*plan, session_context, {}, {}, explain_settings) != PlanPrinter::textLogicalPlan(expected_plan, session_context, {}, {}, explain_settings))
    {
        throw Exception(
            ErrorCodes::OPTIMIZER_TEST_FAILED,
            "assertMatch failed! Result diff: \nActual Plan \n{} \nExcepted Plan \n{}\n",
            PlanPrinter::textLogicalPlan(*plan, session_context, {}, {}, explain_settings),
            PlanPrinter::textLogicalPlan(expected_plan, session_context, {}, {}, explain_settings));
    }
}

void OptimizerTester::assertContains(const QueryPlanExtPtr & plan, const String & expected, QueryPlanSettings explain_settings)
{
    String p = printPlan(plan, explain_settings);
    if (p.find(expected) == String::npos)
    {
        throw Exception(
            ErrorCodes::OPTIMIZER_TEST_FAILED,
            "assertContains failed! Result diff: \nActual Plan \n{} \nExcepted SubPlan \n{}\n",
            p,
            expected);
    }
}

void OptimizerTester::assertNotContains(const QueryPlanExtPtr & plan, const String & expected, QueryPlanSettings explain_settings)
{
    String p = printPlan(plan, explain_settings);
    if (p.find(expected) != String::npos)
    {
        throw Exception(
            ErrorCodes::OPTIMIZER_TEST_FAILED,
            "assertNotContains failed! Result diff: \nActual Plan \n{} \nUnexcepted SubPlan \n{}\n",
            p,
            expected);
    }
}

}
