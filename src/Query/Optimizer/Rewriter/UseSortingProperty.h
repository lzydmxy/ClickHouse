#pragma once

#include <Common/Logger.h>
#include <Core/SortDescription.h>
#include <Interpreters/Context.h>
#include <Query/Optimizer/Property/Property.h>
#include <Query/Optimizer/Property/Constants.h>
#include <Query/Optimizer/Rewriter/Rewriter.h>
#include <Query/Processors/QueryPlan/CTEInfo.h>
#include <Query/Processors/QueryPlan/SimplePlanRewriter.h>
#include <Query/Processors/QueryPlan/SimplePlanVisitor.h>

namespace DB
{

struct PlanAndPropConstants
{
    PlanNodePtr plan;
    Property property;
    Constants constants;
};

class SortingOrderedSource : public Rewriter
{
public:
    String name() const override { return "SortingOrderedSource"; }
private:
    bool rewrite(QueryPlanExt & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override
    {
        return context->getOptimizerContext()->getSettingsRef().enable_sorting_property && context->getSettingsRef().optimize_read_in_order;
    }
    class Rewriter;
};

class SortingOrderedSource::Rewriter : public PlanNodeVisitor<PlanAndPropConstants, SortDescription>
{
public:
    Rewriter(ContextMutablePtr context_, CTEInfo & cte_info_) : context(context_), cte_helper(cte_info_) { }

    PlanAndPropConstants visitPlanNode(PlanNodeBase &, SortDescription & required) override;
    PlanAndPropConstants visitSortingStepExtNode(SortingStepExtNode &, SortDescription & required) override;
    PlanAndPropConstants visitAggregatingStepExtNode(AggregatingStepExtNode &, SortDescription & required) override;
    PlanAndPropConstants visitWindowStepNode(WindowStepNode &, SortDescription & required) override;
    PlanAndPropConstants visitTopNFilteringStepExtNode(TopNFilteringStepExtNode & node, SortDescription & required) override;

    PlanAndPropConstants visitCTERefStepExtNode(CTERefStepExtNode & node, SortDescription & required) override;
    PlanAndPropConstants visitProjectionStepExtNode(ProjectionStepExtNode & node, SortDescription & required) override;
    PlanAndPropConstants visitFilterStepExtNode(FilterStepExtNode & node, SortDescription & required) override;
    PlanAndPropConstants visitTableScanStepExtNode(TableScanStepExtNode & node, SortDescription & required) override;

private:
    ContextMutablePtr context;
    SimpleCTEVisitHelper<PlanAndPropConstants> cte_helper;
};

struct SortInfo
{
    SortDescription sort_desc;
    size_t limit = 0ul;
};

class PruneSortingInfoRewriter : public SimplePlanRewriter<SortInfo>
{
public:
    PruneSortingInfoRewriter(ContextMutablePtr context_, CTEInfo & cte_info_)
        : SimplePlanRewriter(context_, cte_info_), logger(getLogger("PruneSortingInfoRewriter"))
    {
    }

    PlanNodePtr visitPlanNode(PlanNodeBase & node, SortInfo & required) override;
    PlanNodePtr visitSortingStepExtNode(SortingStepExtNode &, SortInfo &) override;
    PlanNodePtr visitAggregatingStepExtNode(AggregatingStepExtNode &, SortInfo &) override;
    // PlanNodePtr visitWindowStepNode(WindowStepNode &, SortInfo &) override;
    PlanNodePtr visitTopNFilteringStepExtNode(TopNFilteringStepExtNode & node, SortInfo &) override;
    PlanNodePtr visitProjectionStepExtNode(ProjectionStepExtNode & node, SortInfo & required) override;
    PlanNodePtr visitTableScanStepExtNode(TableScanStepExtNode &, SortInfo & required) override;

private:
    LoggerPtr logger;
};

}
