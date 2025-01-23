#include <Query/Optimizer/CostModel/CostModel.h>
#include <Query/Optimizer/CostModel/PlanNodeCost.h>

namespace DB
{
PlanNodeCost PlanNodeCost::ZERO(0.0, 0.0, 0.0);

double PlanNodeCost::getCost(const CostModel & cost_model) const
{
    return cpu_value * cost_model.getCPUCostWeight() + mem_value * cost_model.getMemCostWeight() + net_value * cost_model.getNetCostWeight();
}

}
