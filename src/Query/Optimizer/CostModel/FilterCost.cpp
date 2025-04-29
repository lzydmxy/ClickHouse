#include <Query/Optimizer/CostModel/FilterCost.h>

#include <Query/Optimizer/CostModel/CostCalculator.h>

namespace DB
{
PlanNodeCost FilterCost::calculate(const FilterStepExt &, CostContext & context)
{
    PlanNodeStatisticsPtr children_stats = context.children_stats[0];
    if (!children_stats)
        return PlanNodeCost::ZERO;
    return PlanNodeCost::cpuCost(children_stats->getRowCount()) * context.cost_model.getProjectionCostWeight();
}
}
