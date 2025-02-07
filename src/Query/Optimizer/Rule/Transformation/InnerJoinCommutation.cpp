#include <Query/Optimizer/Cascades/CascadesOptimizer.h>
#include <Query/Optimizer/Rule/Patterns.h>
#include <Query/Optimizer/Rule/Transformation/InnerJoinCommutation.h>
#include <QueryPlan/AnyStep.h>

namespace DB
{
ConstRefPatternPtr InnerJoinCommutation::getPattern() const
{
    static auto pattern = Patterns::join()
        .matchingStep<JoinStep>([](const JoinStep & s) { return supportSwap(s) && !s.isOrdered(); })
        .with(Patterns::any(), Patterns::any()).result();
    return pattern;
}

TransformResult InnerJoinCommutation::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto * join_node = dynamic_cast<JoinNode *>(node.get());
    if (!join_node)
        return {};

    return {swap(*join_node, rule_context)};
}

PlanNodePtr InnerJoinCommutation::swap(JoinNode & node, RuleContext & rule_context)
{
    auto & step = *node.getStep();
    DataStreams streams = {step.getInputStreams()[1], step.getInputStreams()[0]};
    auto join_step = std::make_shared<JoinStep>(
        streams,
        step.getOutputStream(),
        ASTTableJoin::Kind::Inner,
        step.getStrictness(),
        step.getMaxStreams(),
        step.getKeepLeftReadInOrder(),
        step.getRightKeys(),
        step.getLeftKeys(),
        step.getKeyIdsNullSafe(),
        step.getFilter(),
        step.isHasUsing(),
        step.getRequireRightKeys(),
        ASOF::Inequality::GreaterOrEquals,
        DistributionType::UNKNOWN,
        JoinAlgorithm::AUTO,
        false,
        step.isOrdered(),
        step.isSimpleReordered(),
        step.getRuntimeFilterBuilders(),
        step.getHints());
    return std::make_shared<JoinNode>(
        rule_context.context->nextNodeId(), std::move(join_step), PlanNodes{node.getChildren()[1], node.getChildren()[0]});
}

const std::vector<RuleType> & InnerJoinCommutation::blockRules() const
{
    static std::vector<RuleType> block{RuleType::INNER_JOIN_COMMUTATION, RuleType::JOIN_ENUM_ON_GRAPH};
    return block;
}

}
