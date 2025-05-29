#pragma once

#include <filesystem>
#include <memory>
#include <type_traits>
#include <unordered_map>
#include <Query/Optimizer/Rewriter/Rewriter.h>
#include <Query/Optimizer/Rule/Rule.h>
#include <Query/Optimizer/tests/gtest_base_unittest_mock.h>
#include <Query/Processors/QueryPlan/CTEInfo.h>
#include <Common/tests/gtest_global_context.h>
#include <Query/Processors//QueryPlan/QueryPlanExt.h>
#include <Query/Optimizer/tests/gtest_base_plan_test.h>
#include <Query/Optimizer/PlanNodeSearcher.h>
#include <Query/Processors/QueryPlan/PlanPrinter.h>
#include <Query/Planner/PlannerExt.h>


#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Disks/registerDisks.h>
#include <Formats/registerFormats.h>
#include <Functions/registerFunctions.h>
#include <Dictionaries/registerDictionaries.h>
#include <Storages/registerStorages.h>
#include <TableFunctions/registerTableFunctions.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int OPTIMIZER_TEST_FAILED;
}

template<typename T>
concept RuleOrRewriter = std::is_base_of_v<Rule, typename T::element_type> || std::is_base_of_v<Rewriter, typename T::element_type> || std::is_same_v<std::remove_cv_t<T>, Rewriters>;

class OptimizerTester : protected BasePlanTest
{
public:
    explicit OptimizerTester(std::unordered_map<std::string, Field> settings = {}) : BasePlanTest("optimierUT", settings)
    {
        session_context = createQueryContext();
    }

    ContextMutablePtr getContext() { return session_context; }

    // assert the rule isn't in effect.
    template <RuleOrRewriter T>
    void assertNotFire(T rule, PlanNodePtr input, CTEInfo cte_info = {}, QueryPlanSettings explain_settings = {.stats = false})
    {
        refresh();
        bool no_result = false;
        if constexpr (std::is_base_of_v<Rule, typename T::element_type>)
        {
            RuleContext rule_context{.context = session_context, .cte_info = cte_info};
            TransformResult result = rule->transform(input, rule_context);
            no_result = result.empty();
        }
        else if constexpr (std::is_base_of_v<Rewriter, typename T::element_type>)
        {
            QueryPlanExt plan = QueryPlanExt{input, cte_info, session_context->getOptimizerContext()->getPlanNodeIdAllocator()};
            String old_plan_text = PlanPrinter::textLogicalPlan(plan, session_context, {}, {}, explain_settings);
            rule->rewritePlan(plan, session_context);

            no_result = PlanPrinter::textLogicalPlan(plan, session_context, {}, {}, explain_settings) == old_plan_text;
        }
        else
        {
            for (const auto & r : rule)
            {
                assertNotFire(r, input, cte_info);
            }
        }

        if (!no_result)
        {
            throw Exception(ErrorCodes::OPTIMIZER_TEST_FAILED, "Assert NotFire failed!");
        }
    }

    QueryPlanExtPtr plan(RulePtr rule, PlanNodePtr input, CTEInfo in_cte_info = {})
    {
        refresh();

        RuleContext rule_context{.context = session_context, .cte_info = in_cte_info};
        TransformResult result = rule->transform(input, rule_context);
        PlanNodePtr rewritten = result.getPlans()[0];
        return std::make_unique<QueryPlanExt>(rewritten, in_cte_info, session_context->getOptimizerContext()->getPlanNodeIdAllocator());
    }

    QueryPlanExtPtr plan(RewriterPtr rule, PlanNodePtr input, CTEInfo in_cte_info = {})
    {
        refresh();

        auto output_plan = std::make_unique<QueryPlanExt>(input, in_cte_info, session_context->getOptimizerContext()->getPlanNodeIdAllocator());
        rule->rewritePlan(*output_plan, session_context);
        return output_plan;
    }

    QueryPlanExtPtr plan(Rewriters rules, PlanNodePtr input, CTEInfo in_cte_info = {})
    {
        refresh();

        auto output_plan = std::make_unique<QueryPlanExt>(input, in_cte_info, session_context->getOptimizerContext()->getPlanNodeIdAllocator());
        for (const auto & rule : rules)
            rule->rewritePlan(*output_plan, session_context);
        return output_plan;
    }

    void registerTable(const String & DDL);

    QueryPlanExtPtr plan(const String & query)
    {
        refresh();
        return BasePlanTest::plan(query, session_context);
    }

    // assert the input QueryPlan is the same with expected QueryPlan.
    void assertMatch(const QueryPlanExtPtr & plan, PlanNodePtr expected_plan_node, CTEInfo expected_cte_info = {}, QueryPlanSettings explain_settings = {.stats = false}) const;

    void assertContains(const QueryPlanExtPtr & plan, const String & expected, QueryPlanSettings explain_settings = {.stats = false});
    void assertNotContains(const QueryPlanExtPtr & plan, const String & expected, QueryPlanSettings explain_settings = {.stats = false});


    ASTPtr parse(const std::string & query)
    {
        return BasePlanTest::parse(query, session_context);
    }

    PlanSegmentTreePtr planSegment(const String & query)
    {
        refresh();
        return BasePlanTest::planSegment(query, session_context);
    }

    std::string execute(const String & query)
    {
        refresh();
        return BasePlanTest::execute(query, session_context);
    }

    String printPlan(QueryPlanExt & plan, QueryPlanSettings explain_settings = {.stats = false})
    {
        refresh();
        return PlanPrinter::textLogicalPlan(plan, session_context, {}, {}, explain_settings);
    }

    // return plan text, just for debug.
    String printPlan(const QueryPlanExtPtr & plan, QueryPlanSettings explain_settings = {.stats = false})
    {
        return printPlan(*plan, explain_settings);
    }

private:
    void refresh() { session_context->getOptimizerContext()->createSymbolAllocator(); }

};

}
