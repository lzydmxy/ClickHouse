#include <Query/Processors/QueryPlan/PlanPattern.h>

#include <Query/Optimizer/JoinGraph.h>
#include <Query/Processors/QueryPlan/JoinStepExt.h>

namespace DB
{
bool PlanPattern::isSimpleQuery(QueryPlanExt & plan)
{
    SimpleQueryPlanPatternVisitor visitor{plan.getCTEInfo()};
    Void context;
    VisitorUtil::accept(plan.getPlanNode(), visitor, context);
    return visitor.isSimpleQuery();
}

bool PlanPattern::hasCrossJoin(QueryPlanExt & plan)
{
    CrossJoinPlanPatternVisitor visitor{plan.getCTEInfo()};
    Void context;
    VisitorUtil::accept(plan.getPlanNode(), visitor, context);
    return visitor.hasCrossJoin();
}

bool PlanPattern::hasOuterJoin(QueryPlanExt & plan)
{
    OuterJoinPlanPatternVisitor visitor{plan.getCTEInfo()};
    Void context;
    VisitorUtil::accept(plan.getPlanNode(), visitor, context);
    return visitor.hasOuterJoin();
}

size_t PlanPattern::maxJoinSize(QueryPlanExt & plan, ContextMutablePtr & context)
{
    GetMaxJoinSizeVisitor visitor{context, plan.getCTEInfo()};
    Void v;
    VisitorUtil::accept(plan.getPlanNode(), visitor, v);
    return visitor.getMaxSize();
}

std::set<QueryPlanStepType> PlanPattern::extractStepTypes(QueryPlanExt & plan)
{
    ExtractTypesVisitor visitor{plan.getCTEInfo()};
    Void v;
    VisitorUtil::accept(plan.getPlanNode(), visitor, v);
    return visitor.getTypes();
}

Void SimpleQueryPlanPatternVisitor::visitJoinStepExtNode(JoinStepExtNode &, Void &)
{
    simple_query = false;
    return Void{};
}

Void SimpleQueryPlanPatternVisitor::visitApplyStepExtNode(ApplyStepExtNode &, Void &)
{
    simple_query = false;
    return Void{};
}

Void SimpleQueryPlanPatternVisitor::visitIntersectOrExceptStepNode(IntersectOrExceptStepNode &, Void &)
{
    simple_query = false;
    return Void{};
}


Void SimpleQueryPlanPatternVisitor::visitCTERefStepExtNode(CTERefStepExtNode &, Void &)
{
    simple_query = false;
    return Void{};
}

Void CrossJoinPlanPatternVisitor::visitJoinStepExtNode(JoinStepExtNode & node, Void & context)
{
    visitPlanNode(node, context);

    const auto & join_step = *node.getStep();

    if (join_step.isCrossJoin())
        has_cross_join = true;

    return Void{};
}

Void OuterJoinPlanPatternVisitor::visitJoinStepExtNode(JoinStepExtNode & node, Void & context)
{
    visitPlanNode(node, context);

    const auto & join_step = *node.getStep();

    if (join_step.getKind() == JoinKind::Left || join_step.getKind() == JoinKind::Right
        || join_step.getKind() == JoinKind::Full)
    {
        has_outer_join = true;
    }
    return Void{};
}

size_t GetMaxJoinSizeVisitor::visitJoinStepExtNode(JoinStepExtNode & node, Void & v)
{
    const auto & step = *node.getStep();
    auto left = VisitorUtil::accept(node.getChildren()[0], *this, v);
    auto right = VisitorUtil::accept(node.getChildren()[1], *this, v);
    if (step.supportReorder(true, false))
    {
        auto size = left + right;
        if (size > max_size)
            max_size = size;
        return size;
    }
    return 1;
}
}
