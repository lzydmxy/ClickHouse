#pragma once

#include <Interpreters/Context.h>
#include <Query/Optimizer/Property/Equivalences.h>
#include <Query/Optimizer/Rewriter/Rewriter.h>
#include <Query/Processors/QueryPlan/SimplePlanVisitor.h>

namespace DB
{
class UnifyJoinOutputs : public Rewriter
{
public:
    String name() const override { return "UnifyJoinOutputs"; }
private:
    bool rewrite(QueryPlanExt & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override
    {
        return context->getOptimizerContext()->getSettingsRef().enable_unify_join_outputs;
    }
    class UnionFindExtractor;
    class Rewriter;
};

class UnifyJoinOutputs::UnionFindExtractor : public SimplePlanVisitor<std::unordered_map<PlanNodeId, UnionFind<String>>>
{
public:
    static std::unordered_map<PlanNodeId, UnionFind<String>> extract(QueryPlanExt & plan);
private:
    explicit UnionFindExtractor(CTEInfo & cte_info) : SimplePlanVisitor(cte_info) { }
    Void visitJoinStepExtNode(JoinStepExtNode &, std::unordered_map<PlanNodeId, UnionFind<String>> &) override;
    Void visitCTERefStepExtNode(CTERefStepExtNode & node, std::unordered_map<PlanNodeId, UnionFind<String>> & context) override;
};

class UnifyJoinOutputs::Rewriter : public PlanNodeVisitor<PlanNodePtr, std::set<String>>
{
public:
    Rewriter(ContextMutablePtr context_, CTEInfo & cte_info_, std::unordered_map<PlanNodeId, UnionFind<String>> & union_find_map_)
        : context(context_), cte_helper(cte_info_), union_find_map(union_find_map_)
    {
    }
    PlanNodePtr visitPlanNode(PlanNodeBase &, std::set<String> &) override;
    PlanNodePtr visitJoinStepExtNode(JoinStepExtNode &, std::set<String> &) override;
    PlanNodePtr visitCTERefStepExtNode(CTERefStepExtNode & node, std::set<String> &) override;

private:
    ContextMutablePtr context;
    SimpleCTEVisitHelper<PlanNodePtr> cte_helper;
    std::unordered_map<PlanNodeId, UnionFind<String>> & union_find_map;
};
}
