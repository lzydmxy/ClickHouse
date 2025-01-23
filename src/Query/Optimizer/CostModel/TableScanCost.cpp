#include <Query/Optimizer/CostModel/CostCalculator.h>
#include <Query/Optimizer/CostModel/TableScanCost.h>

namespace DB
{

PlanNodeCost TableScanCost::calculate(const TableScanStep &, CostContext & context)
{
    if (!context.stats)
        return PlanNodeCost::ZERO;
    return PlanNodeCost::cpuCost( context.cost_model.isEnableUseByteSize() ? context.stats->getOutputSizeInBytes() : context.stats->getRowCount()) * context.cost_model.getTableScanCostWeight();
}

}
