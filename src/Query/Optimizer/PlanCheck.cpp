#include <Query/Optimizer/PlanCheck.h>

#include <Query/Analyzer/TypeAnalyzer.h>

namespace DB
{
void PlanCheck::checkInitPlan(QueryPlanExt & plan, ContextMutablePtr context)
{
    // As init plan may contain correlated symbols, pass check filter.
    SymbolChecker::check(plan, context, false);
}

void PlanCheck::checkFinalPlan(QueryPlanExt & plan, ContextMutablePtr context)
{
    SymbolChecker::check(plan, context, true);
    TableScanChecker::check(plan, context);
}

void ReadNothingChecker::check(PlanNodePtr plan)
{
    // if the whole plan is simplify to ReadNothingNode, return.
    if (getQueryPlanStepType(plan->getStep()) == QueryPlanStepType::ReadNothing)
    {
        return;
    }
    ReadNothingChecker read_nothing_check;
    Void context{};

    // if sub-plan contains ReadNothing Node, throw Exception.
    VisitorUtil::accept(plan, read_nothing_check, context);
}

Void ReadNothingChecker::visitPlanNode(PlanNodeBase & node, Void & context)
{
    for (const auto & item : node.getChildren())
    {
        VisitorUtil::accept(*item, *this, context);
    }
    return {};
}

Void ReadNothingChecker::visitReadNothingNode(ReadNothingNode &, Void &)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "ReadNothingNode must removed in query optimization");
}

void SymbolChecker::check(QueryPlanExt & plan, ContextMutablePtr & context, bool check_filter)
{
    SymbolChecker symbol_check{check_filter};
    VisitorUtil::accept(plan.getPlanNode(), symbol_check, context);
}

Void SymbolChecker::visitPlanNode(PlanNodeBase & node, ContextMutablePtr & context)
{
    for (const auto & item : node.getChildren())
    {
        VisitorUtil::accept(*item, *this, context);
    }
    return {};
}

Void SymbolChecker::visitProjectionStepExtNode(ProjectionStepExtNode & node, ContextMutablePtr & context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, context);
    const auto & step = *node.getStep();
    const auto & assignments = step.getAssignments();
    const auto & input_header = node.getChildren()[0]->getStep()->getOutputStream().header;
    auto names_and_types = input_header.getNamesAndTypes();
    auto type_analyzer = TypeAnalyzer::create(context, names_and_types);
    for (const auto & assignment : assignments)
    {
        ConstASTPtr value = assignment.second;
        type_analyzer.getType(value);
    }
    return {};
}

Void SymbolChecker::visitFilterStepExtNode(FilterStepExtNode & node, ContextMutablePtr & context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, context);

    if (check_filter)
    {
        const auto & step = *node.getStep();
        auto predicate = step.getFilter();
        const auto & input_header = node.getChildren()[0]->getStep()->getOutputStream().header;
        TypeAnalyzer::getType(predicate, context, input_header.getNamesAndTypes());
    }
    return {};
}

void TableScanChecker::check(QueryPlanExt & plan, ContextMutablePtr & context)
{
    TableScanChecker tablescan_check;
    VisitorUtil::accept(plan.getPlanNode(), tablescan_check, context);
}

Void TableScanChecker::visitPlanNode(PlanNodeBase & node, ContextMutablePtr & context)
{
    for (const auto & item : node.getChildren())
    {
        VisitorUtil::accept(*item, *this, context);
    }
    return {};
}

Void TableScanChecker::visitTableScanStepExtNode(TableScanStepExtNode & node, ContextMutablePtr & context)
{
    auto & step = node.getStep();
    if (!context->getOptimizerContext()->getSettingsRef().allow_map_access_without_key && step->getStorage() && step->getStorage()->supportsMapImplicitColumn())
    {
        if (!step->getStorageSnapshot())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "StorageSnapshot is nullptr in TableScan");
        Block header = step->getStorageSnapshot()->getSampleBlockForColumns(step->getRequiredColumns());
        for (auto & col : header)
        {
            if (col.type->isByteMap())
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Map column access without key is not allowed for ByteMap");
        }
    }
    return {};
}
}
