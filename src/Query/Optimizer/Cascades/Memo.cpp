#include <Query/Optimizer/Cascades/Memo.h>

#include <Query/Optimizer/Cascades/CascadesOptimizer.h>
#include <QueryPlan/AnyStep.h>
#include <QueryPlan/MultiJoinStep.h>

namespace DB
{
GroupExprPtr Memo::insertGroupExpr(GroupExprPtr group_expr, CascadesContext & context, GroupId target)
{
    // If leaf, then just return
    if (group_expr->getStep()->getType() == IQueryPlanStep::Type::Any)
    {
        const auto * leaf = dynamic_cast<const AnyStep *>(group_expr->getStep().get());
        group_expr->setGroupId(leaf->getGroupId());
        return nullptr;
    }

    auto it = group_expressions.find(group_expr);
    // duplicate group expression
    if (it != group_expressions.end())
    {
        return it->first;
    }

    // New expression, so try to insert into an existing group or
    // create a new group if none specified
    GroupId group_id;
    if (target == UNDEFINED_GROUP)
    {
        group_id = addNewGroup();
        // LOG_DEBUG(
        //     context.getLog(),
        //     "New Group Id {} Rule Type: {}, group_expr step hash: {}, group_expr hash: {}",
        //     group_id,
        //     group_expr->getProduceRule(),
        //     group_expr->getStep()->hash(),
        //     group_expr->hash());
    }
    else
    {
        group_id = target;
    }
    group_expr->setGroupId(group_id);

    auto group = getGroupById(group_id);
    group->addExpression(group_expr, context);

    // must after group.addexpression because this function can change step
    group_expressions[group_expr] = group_id;
    return group_expr;
}


}
