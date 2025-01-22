#pragma once
#include <Query/Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Processors/QueryPlan/FinalSampleStep.h>

namespace DB
{
class SampleEstimator
{
public:
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const FinalSampleStep &);
};

}
