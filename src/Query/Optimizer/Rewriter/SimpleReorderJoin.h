#pragma once

#include <Interpreters/Context.h>
#include <Query/Optimizer/JoinGraph.h>
#include <Query/Optimizer/Rewriter/Rewriter.h>
#include <Query/Parsers/ASTVisitor.h>
#include <Query/Processors/QueryPlan/PlanVisitor.h>
#include <Query/Processors/QueryPlan/SimplePlanRewriter.h>
#include <Query/Processors/QueryPlan/SimplePlanVisitor.h>

#include <utility>

namespace DB
{
class SimpleReorderJoin : public Rewriter
{
public:
    String name() const override { return "SimpleReorderJoin"; }

private:
    bool rewrite(QueryPlanExt & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override { return context->getOptimizerContext()->getSettingsRef().enable_join_reorder; }
};

class SimpleReorderJoinVisitor : public SimplePlanRewriter<Void>
{
public:
    explicit SimpleReorderJoinVisitor(ContextMutablePtr context_, CTEInfo & cte_info_)
        : SimplePlanRewriter(context_, cte_info_), cte_info(cte_info_)
    {
    }
    PlanNodePtr visitJoinStepExtNode(JoinStepExtNode &, Void &) override;
    static PlanNodePtr
    buildJoinTree(std::vector<String> & expected_output_symbols, JoinGraph & graph, PlanNodePtr join_node, ContextMutablePtr & context_ptr);

private:
    CTEInfo & cte_info;
    std::unordered_set<PlanNodeId> reordered;
    PlanNodePtr getJoinOrder(JoinGraph & graph);
};

struct EdgeSelectivity
{
    PlanNodeId left_id;
    PlanNodeId right_id;
    String left_symbol;
    String right_symbol;
    double selectivity;
    size_t output;
    size_t min_input;
};

struct EdgeSelectivityCompare
{
    bool operator()(const EdgeSelectivity & a, const EdgeSelectivity & b)
    {
        double a_s = a.selectivity > 0.98 ? 1 : a.selectivity;
        double b_s = b.selectivity > 0.98 ? 1 : b.selectivity;
        if (std::fabs(a_s - b_s) >= 1e-7)
            return a_s > b_s;

        if (a.output != b.output)
            return a.output > b.output;

        if (a.min_input != b.min_input)
            return a.min_input > b.min_input;

        if (a.left_symbol != b.left_symbol)
            return a.left_symbol < b.left_symbol;

        return a.right_symbol < b.right_symbol;
    }
};
}
