#pragma once

#include <Query/Optimizer/ExpressionDeterminism.h>
#include <Query/Processors/QueryPlan/PlanVisitor.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW;
}

/**
 * Check query or materialized view is valid for materialized view rewrite.
 * A query is not supported if
 * - query contains nodes except projection, join, table scan, filter.
 * - query contains multiple aggregates.
 * - query contains non-deterministic functions.
 */
class MaterializedViewStepChecker : public StepVisitor<bool, ContextPtr>
{
public:
    static bool isSupported(const QueryPlanStepPtr & step, ContextPtr context)
    {
        static MaterializedViewStepChecker visitor;
        return VisitorUtil::accept(step, visitor, context);
    }

protected:
    bool visitStep(const IQueryPlanStep &, ContextPtr &) override { return false; }

    bool visitAggregatingStepExt(const AggregatingStepExt & step, ContextPtr &) override
    {
        return !step.isGroupingSet();
    }

    bool visitTableScanStepExt(const TableScanStepExt &, ContextPtr &) override { return true; }

    bool visitProjectionStepExt(const ProjectionStepExt & step, ContextPtr & context) override
    {
        for (const auto & assigment : step.getAssignments())
            if (!ExpressionDeterminism::isDeterministic(assigment.second, context))
                return false;
        return !step.isFinalProject();
    }

    bool visitFilterStepExt(const FilterStepExt & step, ContextPtr & context) override
    {
        return ExpressionDeterminism::isDeterministic(step.getFilter(), context);
    }

    bool visitJoinStepExt(const JoinStepExt &, ContextPtr &) override { return true; }
};

class MaterializedViewPlanChecker : public PlanNodeVisitor<void, ContextPtr>
{
public:
    explicit MaterializedViewPlanChecker(bool support_nested_aggregate_) : support_nested_aggregate(support_nested_aggregate_) { }

    void check(PlanNodeBase & node, ContextPtr context) { VisitorUtil::accept(node, *this, context); }

    [[nodiscard]] const PlanNodePtr & getTopAggregateNode() const { return top_aggregate_node; }
    [[nodiscard]] bool hasHavingFilter() const { return has_having_filter; }

protected:
    void visitPlanNode(PlanNodeBase & node, ContextPtr & context) override {
        bool supported = MaterializedViewStepChecker::isSupported(node.getStep(), context);
        if (!supported)
            throw Exception(
                ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW,
                "materialized view query don't support {}", node.getStep()->getName());
        visitChildren(node, context);
    }

    void visitAggregatingStepExtNode(AggregatingStepExtNode & node, ContextPtr & context) override
    {
        if (!allow_aggregate_node)
            throw Exception(
                ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW,
                "materialized view query don't support nested aggregate or aggregate inside join {}",
                node.getStep()->getName());

        has_having_filter = has_filter;
        allow_aggregate_node = support_nested_aggregate;
        top_aggregate_node = node.shared_from_this();
        visitPlanNode(node, context);
    }

    void visitChildren(PlanNodeBase & node, ContextPtr & context)
    {
        if (allow_aggregate_node && node.getChildren().size() > 1)
            allow_aggregate_node = support_nested_aggregate;
        for (const auto & child : node.getChildren())
            VisitorUtil::accept(*child, *this, context);
    }

    void visitSortingStepExtNode(SortingStepExtNode & node, ContextPtr & context) override
    {
        if (dynamic_cast<const SortingStepExt *>(node.getStep().get())->getLimit() != 0)
            throw Exception(
                ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW,
                "materialized view query don't support limit");
        visitChildren(node, context);
    }

    void visitFilterStepExtNode(FilterStepExtNode & node, ContextPtr & context) override
    {
        has_filter = true;
        visitChildren(node, context);
    }

private:
    const bool support_nested_aggregate;
    bool allow_aggregate_node = true;

    bool has_filter = false;
    bool has_having_filter = false;
    PlanNodePtr top_aggregate_node = nullptr;
};
}
