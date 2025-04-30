#pragma once
#include <Query/Processors/QueryPlan/PlanNode.h>
#include <Query/Processors/QueryPlan/PlanVisitor.h>


namespace DB
{
class PlanCheck
{
public:
    static void checkInitPlan(QueryPlanExt & plan, ContextMutablePtr context);
    static void checkFinalPlan(QueryPlanExt & plan, ContextMutablePtr context);
};

class ReadNothingChecker : public PlanNodeVisitor<Void, Void>
{
public:
    static void check(PlanNodePtr plan);

    Void visitPlanNode(PlanNodeBase &, Void &) override;
    // todo: hongzhigao1, ReadNothingNode
    // Void visitReadNothingNode(ReadNothingNode &, Void &) override;
};

class SymbolChecker : public PlanNodeVisitor<Void, ContextMutablePtr>
{
public:
    static void check(QueryPlanExt & plan, ContextMutablePtr & context, bool check_filter);

    explicit SymbolChecker(bool checkFilter) : check_filter(checkFilter) { }

    Void visitPlanNode(PlanNodeBase &, ContextMutablePtr &) override;
    Void visitProjectionStepExtNode(ProjectionStepExtNode &, ContextMutablePtr &) override;
    Void visitFilterStepExtNode(FilterStepExtNode &, ContextMutablePtr &) override;

private:
    bool check_filter;
};

class TableScanChecker : public PlanNodeVisitor<Void, ContextMutablePtr>
{
public:
    static void check(QueryPlanExt & plan, ContextMutablePtr & context);

    Void visitPlanNode(PlanNodeBase &, ContextMutablePtr &) override;
    Void visitTableScanStepExtNode(TableScanStepExtNode &, ContextMutablePtr &) override;
};
}
