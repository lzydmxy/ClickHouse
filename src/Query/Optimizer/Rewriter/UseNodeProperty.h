#pragma once

#include <Interpreters/Context.h>
#include <Query/Optimizer/Property/Property.h>
#include <Query/Optimizer/Rewriter/Rewriter.h>
#include <Query/Processors/QueryPlan/CTEInfo.h>
#include <Query/Processors/QueryPlan/SimplePlanRewriter.h>
#include <Query/Processors/QueryPlan/SimplePlanVisitor.h>

namespace DB
{

class UseNodeProperty : public Rewriter
{
public:
    String name() const override { return "UseNodeProperty"; }

private:
    bool rewrite(QueryPlanExt & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override { return context->getOptimizerContext()->getSettingsRef().enable_use_node_property; }
    class Rewriter;
    class ExchangeRewriter;
};

class UseNodeProperty::ExchangeRewriter : public PlanNodeVisitor<PlanNodePtr, Property>
{
public:
    ExchangeRewriter(ContextMutablePtr context_, CTEInfo & cte_info_) : context(context_), cte_helper(cte_info_) { }
    PlanNodePtr visitPlanNode(PlanNodeBase &, Property &) override;
    PlanNodePtr visitCTERefStepExtNode(CTERefStepExtNode & node, Property &) override;
    PlanNodePtr visitTableScanStepExtNode(TableScanStepExtNode & node, Property &) override;

private:
    ContextMutablePtr context;
    SimpleCTEVisitHelper<PlanNodePtr> cte_helper;
};

}
