#include <Query/Optimizer/Rule/Rewrite/JoinUsingToJoinOn.h>
#include <Query/Optimizer/Rule/Patterns.h>
#include <Query/Processors/QueryPlan/JoinStepExt.h>
#include <Query/Processors/QueryPlan/PlanNode.h>
#include <Query/Processors/QueryPlan/ProjectionStepExt.h>

namespace DB
{
ConstRefPatternPtr JoinUsingToJoinOn::getPattern() const
{
    static auto pattern = Patterns::join().matchingStep<JoinStepExt>([](const JoinStepExt & s) { return s.isHasUsing(); }).result();
    return pattern;
}

TransformResult JoinUsingToJoinOn::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    // currently optimizer not support 'join on null-safe equals'
    // due to performance issues, we don't rewrite null-safe equals to join on
    if (rule_context.context->getOptimizerContext()->getSettingsRef().join_using_null_safe)
        return {};

    auto * join_node = dynamic_cast<JoinStepExtNode *>(node.get());
    if (!join_node)
        return {};

    auto join_step = join_node->getStep();
    auto join_kind = join_step->getKind();

    // when inner/left join, no matter what data in require_right_keys,
    // the using column is ALWAYS same as the left column
    // so we just reset using flag and clear require_right_keys
    //if (join_kind == ASTTableJoin::Kind::Inner || join_kind == ASTTableJoin::Kind::Left)
    if (join_kind == JoinKind::Inner || join_kind == JoinKind::Left)
    {
        auto new_join_step = std::static_pointer_cast<JoinStepExt>(join_step->copy(rule_context.context));
        new_join_step->resetUsing();
        auto new_join_node
            = PlanNodeBase::createPlanNode(node->getId(), std::move(new_join_step), node->getChildren(), node->getStatistics());
        return new_join_node;
    }

    // todo: by, rewrite Outer and Right Join
    return {};
}
}
