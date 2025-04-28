#pragma once
#include <Query/Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Query/Processors/QueryPlan/FinishSortingStepExt.h>
#include <Query/Processors/QueryPlan/MergeSortingStepExt.h>
#include <Query/Processors/QueryPlan/MergingSortedStepExt.h>
#include <Query/Processors/QueryPlan/PartialSortingStepExt.h>
#include <Query/Processors/QueryPlan/SortingStepExt.h>

namespace DB
{
class SortingEstimator
{
public:
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const SortingStepExt &);
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const PartialSortingStepExt &);
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const MergeSortingStepExt &);
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const MergingSortedStepExt &);
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const FinishSortingStepExt &);

};

}
