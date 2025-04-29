#pragma once

#include <Query/Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Query/Processors/QueryPlan/UnionStepExt.h>

namespace DB
{
class UnionEstimator
{
public:
    static PlanNodeStatisticsPtr estimate(std::vector<PlanNodeStatisticsPtr> & child_stats, const UnionStepExt & step);

private:
    static PlanNodeStatisticsPtr mapToOutput(PlanNodeStatistics & child_stats, const std::unordered_map<String, std::vector<String>> & out_to_input, size_t index);
};

}
