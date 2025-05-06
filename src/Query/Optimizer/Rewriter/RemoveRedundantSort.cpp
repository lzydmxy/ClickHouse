#include <Query/Optimizer/Rewriter/RemoveRedundantSort.h>

#include <Query/Processors/QueryPlan/ApplyStepExt.h>
#include <Query/Processors/QueryPlan/CTERefStepExt.h>
#include <Query/Processors/QueryPlan/JoinStepExt.h>
#include <Query/Processors/QueryPlan/ProjectionStepExt.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Parsers/ASTFunction.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
const std::unordered_set<String> RedundantSortVisitor::order_dependent_agg{"groupUniqArray","groupArray","groupArraySample","argMax","argMin","topK","topKWeighted","any","anyLast","anyHeavy",
                                                                           "first_value","last_value","deltaSum","deltaSumTimestamp","groupArrayMovingSum","groupArrayMovingAvg", "groupConcat"};

bool RemoveRedundantSort::rewrite(QueryPlanExt & plan, ContextMutablePtr context) const
{
    RedundantSortVisitor visitor{context, plan.getCTEInfo()};
    RedundantSortContext sort_context{.context = context, .can_sort_be_removed = false};
    auto result = VisitorUtil::accept(plan.getPlanNode(), visitor, sort_context);
    plan.update(result);
    return sort_context.can_sort_be_removed;
}

PlanNodePtr RedundantSortVisitor::visitPlanNode(PlanNodeBase & node, RedundantSortContext & sort_context)
{
    sort_context.can_sort_be_removed = false;
    return SimplePlanRewriter::visitPlanNode(node, sort_context);
}

PlanNodePtr RedundantSortVisitor::processChildren(PlanNodeBase & node, RedundantSortContext & sort_context)
{
    if (node.getChildren().empty())
        return node.shared_from_this();

    PlanNodes children;
    for (const auto & item : node.getChildren())
    {
        RedundantSortContext child_context{.context = sort_context.context, .can_sort_be_removed = sort_context.can_sort_be_removed};
        PlanNodePtr child = VisitorUtil::accept(*item, *this, child_context);
        children.emplace_back(child);
    }

    node.replaceChildren(children);
    return node.shared_from_this();
}

PlanNodePtr RedundantSortVisitor::resetChild(PlanNodeBase & node, PlanNodes & children, RedundantSortContext &)
{
    node.replaceChildren(children);
    return node.shared_from_this();
}

bool RedundantSortVisitor::isOrderDependentAggregateFunction(const String& aggname)
{
    return order_dependent_agg.contains(aggname);
}

bool RedundantSortVisitor::isStateful(ConstASTPtr expression, ContextMutablePtr contextptr)
{
    StatefulVisitor visitor;
    ASTVisitorUtil::accept(expression, visitor, contextptr);
    return visitor.isStateful();
}

PlanNodePtr RedundantSortVisitor::visitProjectionStepExtNode(ProjectionStepExtNode & node, RedundantSortContext & sort_context)
{
    auto step = std::dynamic_pointer_cast<ProjectionStepExt>(node.getStep());
    const Assignments & assignments = step->getAssignments();
    if (sort_context.can_sort_be_removed)
    {
        for (const auto & assignment: assignments)
        {
            if (RedundantSortVisitor::isStateful(assignment.second, sort_context.context))
            {
                sort_context.can_sort_be_removed = false;
                break;
            }
        }
    }

    return processChildren(node, sort_context);
}

PlanNodePtr RedundantSortVisitor::visitAggregatingStepExtNode(AggregatingStepExtNode & node, RedundantSortContext & sort_context)
{
    auto step = node.getStep().get();
    const AggregateDescriptions & descs = step->getAggregates();

    bool is_order_dependent = false;
    for (auto & desc : descs)
    {
        if(isOrderDependentAggregateFunction(desc.function->getName()))
        {
            is_order_dependent = true;
            break;
        }
    }

    sort_context.can_sort_be_removed = !is_order_dependent;
    auto rewritten_child = VisitorUtil::accept(node.getChildren()[0], *this, sort_context);
    PlanNodes children{rewritten_child};
    return resetChild(node, children, sort_context);
}

PlanNodePtr RedundantSortVisitor::visitJoinStepExtNode(JoinStepExtNode & node, RedundantSortContext & sort_context)
{
    sort_context.can_sort_be_removed = true;
    return processChildren(node, sort_context);
}

PlanNodePtr RedundantSortVisitor::visitUnionStepExtNode(UnionStepExtNode & node, RedundantSortContext & sort_context)
{
    sort_context.can_sort_be_removed = true;
    return processChildren(node, sort_context);
}

PlanNodePtr RedundantSortVisitor::visitIntersectOrExceptStepNode(IntersectOrExceptStepNode & node, RedundantSortContext & sort_context)
{
    sort_context.can_sort_be_removed = true;
    return processChildren(node, sort_context);
}

PlanNodePtr RedundantSortVisitor::visitSortingStepExtNode(SortingStepExtNode & node, RedundantSortContext & sort_context)
{
    bool remove_current = sort_context.can_sort_be_removed;
    sort_context.can_sort_be_removed = true;
    auto rewritten_child = VisitorUtil::accept(node.getChildren()[0], *this, sort_context);

    if (remove_current)
        return rewritten_child;

    PlanNodes children{rewritten_child};
    return resetChild(node, children, sort_context);
}

PlanNodePtr RedundantSortVisitor::visitCTERefStepExtNode(CTERefStepExtNode & node, RedundantSortContext & sort_context)
{
    CTEId cte_id = node.getStep()->getId();

    if (cte_require_context.contains(cte_id))
    {
        RedundantSortContext & context = cte_require_context.at(cte_id);
        context.can_sort_be_removed = context.can_sort_be_removed && sort_context.can_sort_be_removed;
    }
    else
    {
        cte_require_context.emplace(cte_id, sort_context);
    }

    RedundantSortContext child_context(cte_require_context.at(cte_id));
    cte_helper.acceptAndUpdate(cte_id, *this, child_context);
    return node.shared_from_this();
}

void StatefulVisitor::visitNode(const ConstASTPtr & node, ContextMutablePtr & context)
{
    for (ConstASTPtr child : node->children)
    {
        ASTVisitorUtil::accept(child, *this, context);
        if (is_stateful)
            return;
    }
}

void StatefulVisitor::visitASTFunction(const ConstASTPtr & node, ContextMutablePtr & context)
{
    auto & fun = node->as<const ASTFunction &>();
    const auto & function = FunctionFactory::instance().tryGet(fun.name, context);
    if (function && function->isStateful())
    {
        is_stateful = true;
        return;
    }
    visitNode(node, context);
}
}
