#pragma once

#include <Query/Optimizer/Rewriter/Rewriter.h>
#include <Query/Processors/QueryPlan/CTEVisitHelper.h>
#include <Query/Processors/QueryPlan/PlanVisitor.h>
#include <Query/Processors/QueryPlan/SimplePlanRewriter.h>

namespace DB
{
class RemoveRedundantDistinct : public Rewriter
{
public:
    String name() const override { return "RemoveRedundantDistinct"; }

private:
    bool rewrite(QueryPlanExt & planExt, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override { return context->getOptimizerContext()->getSettingsRef().enable_distinct_remove; }
};
struct RemoveRedundantAggregateContext
{
    ContextMutablePtr context;
    std::vector<NameSet> distincts;
};
class RemoveRedundantAggregateVisitor : public PlanNodeVisitor<PlanNodePtr, RemoveRedundantAggregateContext>
{
public:
    explicit RemoveRedundantAggregateVisitor(ContextMutablePtr context_, CTEInfo & cte_info, PlanNodePtr &)
        : cte_helper(cte_info), context(context_)
    {
    }

private:
    PlanNodePtr visitDistinctStepExtNode(DistinctStepExtNode & node, RemoveRedundantAggregateContext & context) override;
    PlanNodePtr visitProjectionStepExtNode(ProjectionStepExtNode & node, RemoveRedundantAggregateContext & context) override;
    PlanNodePtr visitLimitByStepNode(LimitByStepNode & node, RemoveRedundantAggregateContext & context) override;
    PlanNodePtr visitFilterStepExtNode(FilterStepExtNode & node, RemoveRedundantAggregateContext & context) override;
    PlanNodePtr visitCTERefStepExtNode(CTERefStepExtNode & node, RemoveRedundantAggregateContext & context) override;
    PlanNodePtr visitJoinStepExtNode(JoinStepExtNode & node, RemoveRedundantAggregateContext & context) override;
    PlanNodePtr visitTableScanStepExtNode(TableScanStepExtNode & node, RemoveRedundantAggregateContext & context) override;
    PlanNodePtr visitAggregatingStepExtNode(AggregatingStepExtNode & node, RemoveRedundantAggregateContext & context) override;
    PlanNodePtr visitPlanNode(PlanNodeBase & node, RemoveRedundantAggregateContext & context) override;
    bool isDistinctNames(const Names &, const NameSet &);
    std::set<std::string> extractSymbol(const ConstASTPtr & node);
    PlanNodePtr resetChildren(PlanNodeBase & node, PlanNodes & children, RemoveRedundantAggregateContext & context);

    SimpleCTEVisitHelper<PlanNodePtr> cte_helper;
    std::unordered_map<CTEId, std::vector<NameSet>> visit_results;
    ContextMutablePtr context;
};
}
