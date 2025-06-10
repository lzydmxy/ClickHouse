#include <Query/Optimizer/Rule/Transformation/LeftJoinToRightJoin.h>

#include <Query/Optimizer/Cascades/CascadesOptimizer.h>
#include <Query/Optimizer/Rule/Patterns.h>

namespace DB
{
ConstRefPatternPtr LeftJoinToRightJoin::getPattern() const
{
    static auto pattern = Patterns::join()
        .matchingStep<JoinStepExt>([](const JoinStepExt & s) { return supportSwap(s) && !s.isOrdered(); })
        .with(Patterns::any(), Patterns::any()).result();
    return pattern;
}

TransformResult LeftJoinToRightJoin::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto *old_join_node = dynamic_cast<JoinStepExtNode *>(node.get());
    if (!old_join_node)
        return {};
    const auto & step = *old_join_node->getStep();

    DataStreams streams = {step.getInputStreams()[1], step.getInputStreams()[0]};
    auto join_step = std::make_shared<JoinStepExt>(
        streams,
        step.getOutputStream(),
        step.getKind() == JoinKind::Left ? JoinKind::Right : step.getKind(),
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
        JoinAlgorithm::DEFAULT,
        false,
        step.isOrdered(),
        step.isSimpleReordered(),
        step.getRuntimeFilterBuilders());
    PlanNodePtr join_node = std::make_shared<JoinStepExtNode>(
        rule_context.context->getOptimizerContext()->nextNodeId(), std::move(join_step), PlanNodes{node->getChildren()[1], node->getChildren()[0]});

    return {join_node};
}
}
