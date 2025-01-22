

#pragma once
#include <Query/Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Processors/QueryPlan/EnforceSingleRowStep.h>

namespace DB
{
class EnforceSingleRowEstimator
{
public:
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const EnforceSingleRowStep & step);
};
}
