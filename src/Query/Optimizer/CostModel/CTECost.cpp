#include <Query/Optimizer/CostModel/CTECost.h>

#include <Query/Optimizer/CostModel/CostCalculator.h>
#include <QueryPlan/CTERefStep.h>

namespace DB
{
PlanNodeCost CTECost::calculate(const CTERefStep &, CostContext & context)
{
    PlanNodeStatisticsPtr stats = context.stats;
    if (!stats)
        return PlanNodeCost::ZERO;

    return context.cost_model.isEnableUseByteSize()
        ? PlanNodeCost::cpuCost(stats->getOutputSizeInBytes()) * context.cost_model.getCTECostWeight()
            + PlanNodeCost::netCost(stats->getOutputSizeInBytes()) + PlanNodeCost::memCost(stats->getOutputSizeInBytes())
        : PlanNodeCost::cpuCost(stats->getRowCount()) * context.cost_model.getCTECostWeight() + PlanNodeCost::netCost(stats->getRowCount())
            + PlanNodeCost::memCost(stats->getRowCount());
}
}
