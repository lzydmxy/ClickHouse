#include <Query/Optimizer/JoinOrderUtils.h>

#include <Query/Optimizer/Property/Property.h>
#include <Query/Optimizer/Property/PropertyDeriver.h>
#include <Query/Optimizer/Property/PropertyMatcher.h>
#include <Query/Optimizer/SymbolsExtractor.h>
#include <Query/Processors/QueryPlan/CTERefStepExt.h>
#include <Query/Planner/SymbolMapper.h>
#include <Query/Processors/QueryPlan/TableScanStepExt.h>

namespace DB
{
String JoinOrderUtils::getJoinOrder(QueryPlanExt & plan)
{
    JoinOrderExtractor rewriter{plan.getCTEInfo()};
    Void require;
    return VisitorUtil::accept(plan.getPlanNode(), rewriter, require);
}

String JoinOrderExtractor::visitPlanNode(PlanNodeBase & node, Void &)
{
    Names children;
    Void require;
    PropertySet input_properties;
    for (const auto & child : node.getChildren())
    {
        auto result = VisitorUtil::accept(child, *this, require);
        children.emplace_back(result);
    }
    if (children.size() == 1)
        return children[0];

    return fmt::format("[{}]", boost::algorithm::join(children, ", "));
}

String JoinOrderExtractor::visitJoinStepExtNode(JoinStepExtNode & node, Void & v)
{
    auto left = VisitorUtil::accept(node.getChildren()[0], *this, v);
    auto right = VisitorUtil::accept(node.getChildren()[1], *this, v);

    return fmt::format("({}, {})", left, right);
}


String JoinOrderExtractor::visitTableScanStepExtNode(TableScanStepExtNode & node, Void &)
{
    return node.getStep()->getTable();
}

String JoinOrderExtractor::visitCTERefStepExtNode(CTERefStepExtNode & node, Void & v)
{
    const auto * step = node.getStep().get();

    auto cte_order = cte_helper.accept(step->getId(), *this, v);

    return cte_order;
}


}
