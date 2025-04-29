

#pragma once
#include <Query/Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Query/Processors/QueryPlan/EnforceSingleRowStepExt.h>

namespace DB
{
class EnforceSingleRowEstimator
{
public:
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const EnforceSingleRowStepExt & step);
};
}
