#include <Query/Optimizer/CostModel/ProjectionCost.h>

#include <Query/Optimizer/CostModel/CostCalculator.h>

namespace DB
{
PlanNodeCost ProjectionCost::calculate(const ProjectionStepExt &, CostContext & context)
{
    PlanNodeStatisticsPtr children_stats = context.children_stats[0];
    if (!children_stats)
        return PlanNodeCost::ZERO;
    return PlanNodeCost::cpuCost(children_stats->getRowCount()) * context.cost_model.getProjectionCostWeight();
}
}
