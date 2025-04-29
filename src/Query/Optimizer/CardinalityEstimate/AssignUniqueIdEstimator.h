

#pragma once

#include <Query/Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Query/Processors/QueryPlan/AssignUniqueIdStepExt.h>

namespace DB
{
class AssignUniqueIdEstimator
{
public:
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const AssignUniqueIdStepExt &);
};

}
