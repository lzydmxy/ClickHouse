#pragma once

#include <Query/Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Query/Processors/QueryPlan/LimitStepExt.h>
#include <Processors/QueryPlan/OffsetStep.h>

namespace DB
{
class LimitEstimator
{
public:
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const LimitStepExt &);
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const LimitByStep &);
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const OffsetStep &);

    static PlanNodeStatisticsPtr getLimitStatistics(PlanNodeStatisticsPtr & child_stats, size_t limit);
};

}
