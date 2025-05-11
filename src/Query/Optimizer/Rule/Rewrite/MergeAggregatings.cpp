#include <Query/Optimizer/Rule/Rewrite/MergeAggregatings.h>
#include <Query/Optimizer/Rule/Pattern.h>
#include <Query/Optimizer/Rule/Patterns.h>
#include <Query/Processors/QueryPlan/AggregatingStepExt.h>
#include <Query/Processors/QueryPlan/QueryPlanExt.h>
//#include <Query/Processors/QueryPlan/QueryPlan.h>

namespace DB
{

ConstRefPatternPtr MergeAggregatings::getPattern() const
{
    static auto pattern = Patterns::aggregating()
        .matchingStep<AggregatingStepExt>([](const AggregatingStepExt & step) { return step.getAggregates().empty(); })
        .withSingle(Patterns::aggregating()).result();
    return pattern;
}

TransformResult MergeAggregatings::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    const auto & parent = dynamic_cast<const AggregatingStepExt &>(*node->getStep().get());
    const auto & children = dynamic_cast<const AggregatingStepExt &>(*node->getChildren()[0]->getStep().get());

    NameSet children_group_keys{children.getKeys().begin(), children.getKeys().end()};

    bool all_contains = std::all_of(
        parent.getKeys().begin(), parent.getKeys().end(), [&](const auto & name) { return children_group_keys.contains(name); });
    if (all_contains)
    {
        return PlanNodeBase::createPlanNode(
            rule_context.context->getOptimizerContext()->nextNodeId(), parent.copy(rule_context.context), node->getChildren()[0]->getChildren());
    }

    return {};
}

}
