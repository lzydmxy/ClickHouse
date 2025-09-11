#include "RemoveFinalStepRules.h"
#include <Query/Optimizer/Rule/Patterns.h>


namespace DB
{

namespace
{
    bool isShardingKeysAndGroupByKeysMatching(const Names & sharding_keys, const Names & group_by_keys)
    {
        if (sharding_keys.size() != group_by_keys.size())
            return false;

        // We should not warry about alias in group by keys, for the group by keys will remove the alias
        // For example:  select k as k1, sum(v) from t3 group by k1; the group by key is 'k'
        std::unordered_set<std::string> expr_columns;
        for (auto & key : group_by_keys)
        {
            expr_columns.emplace(key);
        }

        for (const auto & column : sharding_keys)
        {
            if (!expr_columns.contains(column))
                return false;
        }

        return true;
    }
}

bool RemoveFinalAggStep::isEnabled(ContextPtr context) const
{
    // TODO wujianchao add flag sharding_key_is_deterministic
    bool has_sharding_key = !context->getOptimizerContext()->getShardingKeys().empty();
    auto & settings = context->getSettingsRef();
    return settings.optimize_distributed_group_by_sharding_key && context->getOptimizerContext()->getSettings().enable_remove_final_agg && has_sharding_key;
}

ConstRefPatternPtr RemoveFinalAggStep::getPattern() const
{
    /// TODO wujianchao There is logical error in Pattern matching for more than 2 level query plan, maybe we should fix it.
    // static auto pattern = Patterns::any().withSingle(Patterns::mergingAggregated())
    //     .withSingle(Patterns::exchange()).withSingle(Patterns::aggregating()).result();

    static auto pattern
        = Patterns::any()
              .withSingle(Patterns::mergingAggregated().withSingle(Patterns::exchange().withSingle(Patterns::aggregating())))
              .result();
    return pattern;
}

TransformResult RemoveFinalAggStep::transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context)
{
    auto * merging_agg_step = dynamic_cast<const MergingAggregatedStepExt *>(node->getChildren()[0]->getStep().get());

    // TODO check LIMIT BY

    if (!merging_agg_step->getGroupings().empty()) // skip if cube, rollup, TODO wujianchao totals
        return {};

    if (isShardingKeysAndGroupByKeysMatching(context.context->getOptimizerContext()->getShardingKeys(), merging_agg_step->getKeys()))
    {
        auto & aggregating_node = node->getChildren()[0]->getChildren()[0]->getChildren()[0];
        auto * aggregating_step = dynamic_cast<const AggregatingStepExt *>(node->getChildren()[0]->getChildren()[0]->getChildren()[0]->getStep().get());

        QueryPlanStepPtr new_aggregating_step = std::make_shared<AggregatingStepExt>(
                aggregating_node->getChildren()[0]->getStep()->getOutputStream(),
                aggregating_step->getKeys(),
                aggregating_step->getKeysNotHashed(),
                aggregating_step->getAggregates(),
                aggregating_step->getGroupingSetsParams(),
                true,
                AggregateStagePolicy::DEFAULT,
                aggregating_step->getGroupBySortDescription(),
                aggregating_step->getGroupings(),
                aggregating_step->needOverflowRow(),
                false,
                aggregating_step->isNoShuffle(),
                aggregating_step->isStreamingForCache(),
                aggregating_step->isGroupByUseNulls());

        auto new_aggregating_node = PlanNodeBase::createPlanNode(
            context.context->getOptimizerContext()->nextNodeId(),
            std::move(new_aggregating_step),
            PlanNodes{aggregating_node->getChildren()[0]}
        );

        PlanNodePtr new_child_node;
        if (dynamic_cast<const ExchangeStepExt *>(node->getStep().get()))
        {
            // also remove the exchange node below merging aggregated node
            new_child_node = new_aggregating_node;
        }
        else
        {
            auto new_exchange_step = QueryPlanStepHelper::copyQueryPlanStep(node->getStep(), context.context);
            auto new_exchange_node = PlanNodeBase::createPlanNode(
            context.context->getOptimizerContext()->nextNodeId(),
            std::move(new_exchange_step),
            PlanNodes{new_aggregating_node});

            new_child_node = new_exchange_node;
        }

        auto new_step = QueryPlanStepHelper::copyQueryPlanStep(node->getStep(), context.context);
        auto new_node = PlanNodeBase::createPlanNode(
            context.context->getOptimizerContext()->nextNodeId(),
            std::move(new_step),
            PlanNodes{new_child_node}
        );
        return TransformResult{new_node};
    }
    return {};
}

}
