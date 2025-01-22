

#pragma once

#include <Query/Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Processors/QueryPlan/AssignUniqueIdStep.h>

namespace DB
{
class AssignUniqueIdEstimator
{
public:
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const AssignUniqueIdStep &);
};

}
