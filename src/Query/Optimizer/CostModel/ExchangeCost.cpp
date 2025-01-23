#include <Query/Optimizer/CostModel/ExchangeCost.h>

#include <Query/Optimizer/CostModel/CostCalculator.h>
#include <QueryPlan/ExchangeStep.h>

namespace DB
{
PlanNodeCost ExchangeCost::calculate(const ExchangeStep & step, CostContext & context)
{
    // if shuffle cost is bigger then no shuffle.
    double base_cost = 1;

    // more shuffle keys is better than less shuffle keys.
    // todo data skew
    if (!step.getSchema().getColumns().empty()
        && (step.getSchema().getHandle() == Partitioning::Handle::FIXED_HASH
            || step.getSchema().getHandle() == Partitioning::Handle::BUCKET_TABLE))
        base_cost += 1.0 / (step.getSchema().getColumns().size() + 1);

    if (step.getSchema().getHandle() == Partitioning::Handle::BUCKET_TABLE)
        base_cost *= 1.1;

    if (!context.stats)
        return PlanNodeCost::netCost(base_cost);

    if (step.getSchema().getHandle() == Partitioning::Handle::FIXED_ARBITRARY)
        return PlanNodeCost::ZERO;

    auto single_worker_cost = context.cost_model.isEnableUseByteSize() ? context.stats->getOutputSizeInBytes() : context.stats->getRowCount() + base_cost;
    return PlanNodeCost::netCost(
        step.getSchema().getHandle() == Partitioning::Handle::FIXED_BROADCAST ? single_worker_cost * context.worker_size
                                                                                          : single_worker_cost);
}

}
