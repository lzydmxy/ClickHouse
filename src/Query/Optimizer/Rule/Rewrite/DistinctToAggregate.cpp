#include <Query/Optimizer/Rule/Rewrite/DistinctToAggregate.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/SortDescription.h>
#include <Query/Optimizer/Rule/Patterns.h>
#include <Query/Processors/QueryPlan/AggregatingStepExt.h>
#include <Query/Processors/QueryPlan/DistinctStepExt.h>

namespace DB
{
ConstRefPatternPtr DistinctToAggregate::getPattern() const
{
     static auto pattern = Patterns::distinct().result();
     return pattern;
}

TransformResult DistinctToAggregate::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto distinct_node = dynamic_cast<DistinctStepExtNode *>(node.get());
    if (!distinct_node)
        return {};

    const auto & step = *distinct_node->getStep();

    if (step.getLimitHint() == 0)
    {
        NameSet name_set{step.getColumns().begin(), step.getColumns().end()};
        NamesAndTypes arbitrary_names;

        AggregateDescriptions descriptions;
        for (auto & name_and_type : arbitrary_names)
        {
            // for rare case, distinct columns don't contain all columns outputs.
            AggregateDescription aggregate_desc;
            aggregate_desc.column_name = name_and_type.name;
            aggregate_desc.argument_names = {name_and_type.name};
            AggregateFunctionProperties properties;
            Array parameters;
            aggregate_desc.function = AggregateFunctionFactory::instance().get("any",  NullsAction::EMPTY, {name_and_type.type}, parameters, properties);
            descriptions.emplace_back(aggregate_desc);
        }

        auto group_agg_step = std::make_shared<AggregatingStepExt>(node->getStep()->getOutputStream(), step.getColumns(), NameSet{}, descriptions, GroupingSetsParamsExtList{}, true);
        auto group_agg_node = PlanNodeBase::createPlanNode(rule_context.context->getOptimizerContext()->nextNodeId(), std::move(group_agg_step), node->getChildren());
        return group_agg_node;
    }

    return {};
}

}
