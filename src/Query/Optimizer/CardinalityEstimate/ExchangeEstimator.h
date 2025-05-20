

#pragma once
#include <Query/Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Query/Processors/QueryPlan/ExchangeStepExt.h>

namespace DB
{
class ExchangeEstimator
{
public:
    static PlanNodeStatisticsPtr estimate(std::vector<PlanNodeStatisticsPtr> & child_stats, const ExchangeStepExt & step);

private:
    static PlanNodeStatisticsPtr
    mapToOutput(PlanNodeStatisticsPtr & child_stats, const std::unordered_map<String, std::vector<String>> & out_to_input, size_t index);
};
};
