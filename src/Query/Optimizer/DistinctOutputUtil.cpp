#include <Query/Optimizer/DistinctOutputUtil.h>

// #include <Query/Processors/QueryPlan/ExceptStepExt.h>
// #include <Query/Processors/QueryPlan/IntersectStepExt.h>
#include <Query/Processors/QueryPlan/LimitStepExt.h>
#include <Query/Processors/QueryPlan/MergingSortedStepExt.h>
#include <Query/Processors/QueryPlan/ValuesStepExt.h>

namespace DB
{
bool DistinctOutputQueryUtil::isDistinct(PlanNodeBase & node)
{
    IsDistinctPlanVisitor visitor;
    Void context;
    return VisitorUtil::accept(node, visitor, context);
}

bool IsDistinctPlanVisitor::visitPlanNode(PlanNodeBase &, Void &)
{
    return false;
}

bool IsDistinctPlanVisitor::visitValuesStepExtNode(ValuesStepExtNode & node, Void &)
{
    return dynamic_cast<const ValuesStepExt *>(node.getStep().get())->getRows() <= 1;
}

bool IsDistinctPlanVisitor::visitLimitStepExtNode(LimitStepExtNode & /*node*/, Void &)
{
    // todo: hongzhigao1, getLimitValue
    // const auto * step = dynamic_cast<const LimitStep *>(node.getStep().get());
    // return step->getLimitValue() <= 1;
    return false;
}

// todo: hongzhigao1, IntersectStep
// bool IsDistinctPlanVisitor::visitIntersectNode(IntersectNode & node, Void & context)
// {
//     if (dynamic_cast<const IntersectStep *>(node.getStep().get())->isDistinct())
//         return true;

//     for (auto & child : node.getChildren())
//     {
//         if (!VisitorUtil::accept(child, *this, context))
//             return false;
//     }
//     return true;
// }

bool IsDistinctPlanVisitor::visitEnforceSingleRowStepExtNode(EnforceSingleRowStepExtNode &, Void &)
{
    return true;
}

bool IsDistinctPlanVisitor::visitAggregatingStepExtNode(AggregatingStepExtNode &, Void &)
{
    return true;
}

bool IsDistinctPlanVisitor::visitAssignUniqueIdStepExtNode(AssignUniqueIdStepExtNode &, Void &)
{
    return true;
}

bool IsDistinctPlanVisitor::visitFilterStepExtNode(FilterStepExtNode & node, Void & context)
{
    return VisitorUtil::accept(node.getChildren()[0], *this, context);
}

bool IsDistinctPlanVisitor::visitDistinctStepExtNode(DistinctStepExtNode &, Void &)
{
    return true;
}

// todo: hongzhigao1, ExceptStep
// bool IsDistinctPlanVisitor::visitExceptNode(ExceptNode & node, Void & context)
// {
//     return dynamic_cast<const ExceptStep *>(node.getStep().get())->isDistinct() || VisitorUtil::accept(node.getChildren()[0], *this, context);
// }

bool IsDistinctPlanVisitor::visitMergingSortedStepExtNode(MergingSortedStepExtNode & node, Void &)
{
    return dynamic_cast<const MergingSortedStepExt *>(node.getStep().get())->getLimit() <= 1;
}

}
