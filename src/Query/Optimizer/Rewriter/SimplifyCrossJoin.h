#pragma once

#include <Interpreters/Context.h>
#include <Query/Optimizer/JoinGraph.h>
#include <Query/Optimizer/Rewriter/Rewriter.h>
#include <Query/Parsers/ASTVisitor.h>
#include <Query/Processors/QueryPlan/PlanVisitor.h>
#include <Query/Processors/QueryPlan/SimplePlanRewriter.h>

#include <utility>

namespace DB
{
class SimplifyCrossJoin : public Rewriter
{
public:
    String name() const override { return "SimplifyCrossJoin"; }

private:
    bool rewrite(QueryPlanExt & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override { return context->getSettingsRef().eliminate_cross_joins; }
};

class SimplifyCrossJoinVisitor : public SimplePlanRewriter<Void>
{
public:
    explicit SimplifyCrossJoinVisitor(ContextMutablePtr context_, CTEInfo & cte_info) : SimplePlanRewriter(context_, cte_info) { }
    PlanNodePtr visitJoinStepExtNode(JoinStepExtNode &, Void &) override;
    bool isRewritten() { return rewritten; }

private:
    std::unordered_set<PlanNodeId> reordered;
    static bool isOriginalOrder(std::vector<UInt32> & join_order);
    static std::vector<UInt32> getJoinOrder(JoinGraph & graph);
    PlanNodePtr buildJoinTree(JoinStepExtNode & node, std::vector<String> & expected_output_symbols, JoinGraph & graph, std::vector<UInt32> & join_order);
    bool rewritten = false;
};

class ComparePlanNode
{
public:
    std::unordered_map<PlanNodeId, UInt32> priorities;

    explicit ComparePlanNode(std::unordered_map<PlanNodeId, UInt32> priorities_) : priorities(std::move(priorities_)) { }

    bool operator()(const PlanNodePtr & node1, const PlanNodePtr & node2)
    {
        PlanNodeId id1 = node1->getId();
        PlanNodeId id2 = node2->getId();
        UInt32 value1 = priorities[id1];
        UInt32 value2 = priorities[id2];
        return value1 > value2;
    }
};

}
