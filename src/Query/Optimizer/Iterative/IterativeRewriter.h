#pragma once

#include <Query/Optimizer/Rewriter/Rewriter.h>
#include <Query/Optimizer/Rule/Rule.h>
#include <Query/Processors/QueryPlan/CTEInfo.h>

#include <unordered_map>
#include <vector>
#include <map>

namespace DB
{

// use a linked multi map to make rule application process more reasonable
using RuleIndex = std::unordered_map<QueryPlanStepType, std::vector<RulePtr>>;

struct IterativeRewriterContext
{
    ContextMutablePtr globalContext;
    CTEInfo & cte_info;
    UInt64 optimizer_timeout;
    ExcludedRulesMap * excluded_rules_map;
    Stopwatch watch{CLOCK_THREAD_CPUTIME_ID};
    // for debugging
    QueryPlanExt & plan;
    int rule_apply_count = 0;
};

/**
 * A IterativeOptimizer will loop to apply `Rule`s recursively until
 * the plan does not change or the optimizer timeout been exhausted.
 */
class IterativeRewriter : public Rewriter
{
public:
    IterativeRewriter(const std::vector<RulePtr> & rules_, std::string name_);
    static std::map<std::underlying_type_t<RuleType>, size_t> getRuleCallTimes();
    String name() const override { return names; }
private:
    bool isEnabled(ContextMutablePtr context) const override { return context->getOptimizerContext()->getSettingsRef().enable_iterative_rewriter; }
    bool rewrite(QueryPlanExt & plan, ContextMutablePtr context) const override;

    String names;
    RuleIndex rules;

    bool explorePlan(PlanNodePtr & plan, IterativeRewriterContext & context) const;
    bool exploreNode(PlanNodePtr & node, IterativeRewriterContext & context) const;
    bool exploreChildren(PlanNodePtr & plan, IterativeRewriterContext & context) const;

    static void checkTimeoutNotExhausted(const String & rule_name, const IterativeRewriterContext & context);
};

}
