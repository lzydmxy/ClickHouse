#include <Query/Optimizer/Cascades/CascadesOptimizer.h>
#include <Query/Optimizer/Rule/Patterns.h>
#include <Query/Optimizer/Rule/Transformation/LeftJoinToRightJoin.h>
#include <QueryPlan/AnyStep.h>

namespace DB
{
ConstRefPatternPtr LeftJoinToRightJoin::getPattern() const
{
    static auto pattern = Patterns::join()
        .matchingStep<JoinStep>([](const JoinStep & s) { return supportSwap(s) && !s.isOrdered(); })
        .with(Patterns::any(), Patterns::any()).result();
    return pattern;
}

TransformResult LeftJoinToRightJoin::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto *old_join_node = dynamic_cast<JoinNode *>(node.get());
    if (!old_join_node)
        return {};
    const auto & step = *old_join_node->getStep();

    DataStreams streams = {step.getInputStreams()[1], step.getInputStreams()[0]};
    auto join_step = std::make_shared<JoinStep>(
        streams,
        step.getOutputStream(),
        step.getKind() == ASTTableJoin::Kind::Left ? ASTTableJoin::Kind::Right : step.getKind(),
        step.getStrictness(),
        step.getMaxStreams(),
        step.getKeepLeftReadInOrder(),
        step.getRightKeys(),
        step.getLeftKeys(),
        step.getKeyIdsNullSafe(),
        step.getFilter(),
        step.isHasUsing(),
        step.getRequireRightKeys(),
        step.getAsofInequality(),
        step.getDistributionType(),
        JoinAlgorithm::AUTO,
        false,
        step.isOrdered(),
        step.isSimpleReordered(),
        step.getRuntimeFilterBuilders(),
        step.getHints());
    PlanNodePtr join_node = std::make_shared<JoinNode>(
        rule_context.context->nextNodeId(), std::move(join_step), PlanNodes{node->getChildren()[1], node->getChildren()[0]});

    return {join_node};
}
}
