#pragma once

#include <Query/Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Query/Processors/QueryPlan/FinalSampleStepExt.h>

namespace DB
{
class SampleEstimator
{
public:
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const FinalSampleStepExt &);
};

}
