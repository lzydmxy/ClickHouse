#include <Query/Optimizer/CostModel/AggregatingCost.h>
#include <Query/Optimizer/CostModel/CostCalculator.h>
#include <QueryPlan/AggregatingStep.h>

namespace DB
{
PlanNodeCost AggregatingCost::calculate(const AggregatingStep & step, CostContext & context)
{
    PlanNodeStatisticsPtr stats = context.stats;
    if (!step.isFinal())
        return PlanNodeCost::ZERO;

    PlanNodeStatisticsPtr children_stats = context.children_stats[0];

    if (!stats || !children_stats)
        return PlanNodeCost::ZERO;

    PlanNodeCost input_cost = PlanNodeCost::cpuCost(context.cost_model.isEnableUseByteSize() ? children_stats->getOutputSizeInBytes() : children_stats->getRowCount());
    PlanNodeCost build_cost = PlanNodeCost::cpuCost(stats->getRowCount()) * context.cost_model.getAggregateCostWeight();

    PlanNodeCost mem_cost = PlanNodeCost::memCost(stats->getRowCount() * step.getAggregates().size());
    return input_cost + build_cost + mem_cost;
}
}

