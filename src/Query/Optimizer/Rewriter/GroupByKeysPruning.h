#pragma once


#include <Interpreters/Context.h>
#include <Query/Optimizer/DataDependency/DataDependency.h>
#include <Query/Optimizer/Property/Constants.h>
#include <Query/Optimizer/Rewriter/Rewriter.h>
#include <Query/Processors/QueryPlan/SimplePlanRewriter.h>
#include <Query/Processors/QueryPlan/SimplePlanVisitor.h>

namespace DB
{

class GroupByKeysPruning : public Rewriter
{
public:
    String name() const override { return "GroupByKeysPruning"; }

private:
    bool rewrite(QueryPlanExt & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override { return context->getOptimizerContext()->getSettingsRef().enable_group_by_keys_pruning; }
    class Rewriter;
};

struct PlanAndDataDependencyWithConstants
{
    PlanNodePtr plan;
    DataDependency data_dependency;
    Constants constants;
};

class GroupByKeysPruning::Rewriter : public PlanNodeVisitor<PlanAndDataDependencyWithConstants, Void>
{
public:
    explicit Rewriter(ContextMutablePtr context_, CTEInfo & cte_info_) : context(context_), cte_helper(cte_info_) { }
    PlanAndDataDependencyWithConstants visitPlanNode(PlanNodeBase &, Void &) override;
    PlanAndDataDependencyWithConstants visitAggregatingStepExtNode(AggregatingStepExtNode &, Void &) override;
    PlanAndDataDependencyWithConstants visitCTERefStepExtNode(CTERefStepExtNode &, Void &) override;

private:
    ContextMutablePtr context;
    SimpleCTEVisitHelper<PlanAndDataDependencyWithConstants> cte_helper;
};


}
