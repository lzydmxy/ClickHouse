#include <Query/Optimizer/CardinalityEstimate/LimitEstimator.h>
#include <Query/Optimizer/CardinalityEstimate/SortingEstimator.h>

namespace DB
{
PlanNodeStatisticsPtr SortingEstimator::estimate(PlanNodeStatisticsPtr & child_stats, const SortingStepExt & step)
{
    if (step.getLimit() > 0)
    {
        return LimitEstimator::getLimitStatistics(child_stats, step.getLimit());
    }
    return child_stats;
}

PlanNodeStatisticsPtr SortingEstimator::estimate(PlanNodeStatisticsPtr & child_stats, const PartialSortingStepExt & step)
{
    if (step.getLimit() > 0)
    {
        return LimitEstimator::getLimitStatistics(child_stats, step.getLimit());
    }
    return child_stats;
}

PlanNodeStatisticsPtr SortingEstimator::estimate(PlanNodeStatisticsPtr & child_stats, const MergeSortingStepExt & step)
{
    if (step.getLimit() > 0)
    {
        return LimitEstimator::getLimitStatistics(child_stats, step.getLimit());
    }
    return child_stats;
}

PlanNodeStatisticsPtr SortingEstimator::estimate(PlanNodeStatisticsPtr & child_stats, const MergingSortedStepExt & step)
{
    if (step.getLimit() > 0)
    {
        return LimitEstimator::getLimitStatistics(child_stats, step.getLimit());
    }
    return child_stats;
}

PlanNodeStatisticsPtr SortingEstimator::estimate(PlanNodeStatisticsPtr & child_stats, const FinishSortingStepExt & step)
{
    if (step.getLimit() > 0)
    {
        return LimitEstimator::getLimitStatistics(child_stats, step.getLimit());
    }
    return child_stats;
}

}
