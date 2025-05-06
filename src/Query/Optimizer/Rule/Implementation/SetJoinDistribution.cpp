#include <Query/Optimizer/Rule/Implementation/SetJoinDistribution.h>

#include <Query/Optimizer/Cascades/CascadesOptimizer.h>
#include <Query/Optimizer/Rule/Patterns.h>
#include <Query/Processors/QueryPlan/JoinStepExt.h>

#include <memory>

namespace DB
{
ConstRefPatternPtr SetJoinDistribution::getPattern() const
{
    static auto pattern = Patterns::join()
    .matchingStep<JoinStepExt>([](const JoinStepExt & s) { return s.getDistributionType() == DistributionType::UNKNOWN; })
    .with(Patterns::any(), Patterns::any())
    .result();
    return pattern;
}

TransformResult SetJoinDistribution::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    PlanNodes result;
    auto * join_node = dynamic_cast<JoinStepExtNode *>(node.get());
    if (!join_node)
        return {};

    const auto & step = *join_node->getStep();

    // todo: lizhuoyu5, need statistic
    auto left_group_id = dynamic_cast<const AnyStepExt *>(node->getChildren()[0]->getStep().get())->getGroupId();
    auto left_stats = context.optimization_context->getMemo().getGroupById(left_group_id)->getStatistics();
    auto right_group_id = dynamic_cast<const AnyStepExt *>(node->getChildren()[1]->getStep().get())->getGroupId();
    auto right_stats = context.optimization_context->getMemo().getGroupById(right_group_id)->getStatistics();

    bool need_parallel_hash = false;

    auto construct_renode = [&](DistributionType type) -> PlanNodePtr
    {
        auto re_step = std::dynamic_pointer_cast<JoinStepExt>(QueryPlanStepHelper::copyQueryPlanStep(node->getStep(), context.context));
        re_step->setDistributionType(type);
        if (need_parallel_hash)
            re_step->setJoinAlgorithm(JoinAlgorithm::PARALLEL_HASH);
        return {PlanNodeBase::createPlanNode(context.context->getOptimizerContext()->nextNodeId(), std::move(re_step), node->getChildren())};
    };

    if (right_stats)
    {
        double max_ndv = -1;
        for (const auto & right_key : step.getRightKeys())
        {
            if (right_stats.value()->getSymbolStatistics().contains(right_key))
            {
                max_ndv = std::max(max_ndv, double(right_stats.value()->getSymbolStatistics(right_key)->getNdv()));
            }
        }

        if (!step.getRightKeys().empty() && right_stats.value()->getRowCount() > context.context->getOptimizerContext()->getSettingsRef().parallel_join_threshold)
        {
            need_parallel_hash = true;
        }

        if (max_ndv > context.context->getOptimizerContext()->getSettingsRef().max_replicate_build_size
            || right_stats.value()->getRowCount() > context.context->getOptimizerContext()->getSettingsRef().max_replicate_shuffle_size)
        {
            return construct_renode(DistributionType::REPARTITION);
        }
    }

    if (step.mustRepartition())
    {
        return {construct_renode(DistributionType::REPARTITION)};
    }

    if (step.mustReplicate())
    {
        return {construct_renode(DistributionType::BROADCAST)};
    }

    // when statistics exists, enum both repartition-join and replicated-join.
    if (left_stats && right_stats)
    {
        if (context.context->getOptimizerContext()->getSettingsRef().enum_repartition)
        {
            result.emplace_back(construct_renode(DistributionType::REPARTITION));
        }
        if (context.context->getOptimizerContext()->getSettingsRef().enum_replicate)
        {
            result.emplace_back(construct_renode(DistributionType::BROADCAST));
        }
    }
    else
    // when statistics not exists, default enum replicated-join.
    {
        if (context.context->getOptimizerContext()->getSettingsRef().enum_replicate_no_stats)
        {
            result.emplace_back(construct_renode(DistributionType::BROADCAST));
        }
        else
        {
            result.emplace_back(construct_renode(DistributionType::REPARTITION));
        }
    }

    return TransformResult{result};
}

}
