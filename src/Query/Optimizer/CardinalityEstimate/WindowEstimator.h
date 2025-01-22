

#pragma once
#include <Query/Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Processors/QueryPlan/WindowStep.h>

namespace DB
{
class WindowEstimator
{
public:
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const WindowStep & step);
};
};
