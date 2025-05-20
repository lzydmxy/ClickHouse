#include <Query/Optimizer/CardinalityEstimate/LimitEstimator.h>

namespace DB
{
PlanNodeStatisticsPtr LimitEstimator::estimate(PlanNodeStatisticsPtr & child_stats, const LimitStepExt & step)
{
    /// diff: bc supports prepared param
    // return step.hasPreparedParam() ? child_stats : getLimitStatistics(child_stats, step.getLimit());
    return getLimitStatistics(child_stats, step.getLimit());
}

PlanNodeStatisticsPtr LimitEstimator::estimate(PlanNodeStatisticsPtr & child_stats, const LimitByStep & step)
{
    size_t limit = step.getGroupLength();
    return getLimitStatistics(child_stats, limit);
}

PlanNodeStatisticsPtr LimitEstimator::estimate(PlanNodeStatisticsPtr & child_stats, const OffsetStep & offset)
{
    if (!child_stats)
    {
        return nullptr;
    }

    if (child_stats->getRowCount() <= offset.getOffset())
    {
        return std::make_shared<PlanNodeStatistics>(0, child_stats->getSymbolStatistics());
    }

    return std::make_shared<PlanNodeStatistics>(child_stats->getRowCount() - offset.getOffset(), child_stats->getSymbolStatistics());
}

PlanNodeStatisticsPtr LimitEstimator::getLimitStatistics(PlanNodeStatisticsPtr & child_stats, size_t limit)
{
    if (!child_stats)
    {
        return std::make_shared<PlanNodeStatistics>(limit);
    }

    if (child_stats->getRowCount() <= limit)
    {
        return child_stats->copy();
    }

    return std::make_shared<PlanNodeStatistics>(limit, child_stats->getSymbolStatistics());
}

}
