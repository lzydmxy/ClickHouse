

#pragma once

#include <Query/Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>

namespace DB
{
class AggregateEstimator
{
public:
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const AggregatingStep &, ContextMutablePtr context);
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const MergingAggregatedStep &, ContextMutablePtr context);
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const DistinctStep &, ContextMutablePtr context);

private:
    static SymbolStatisticsPtr estimateAggFun(AggregateFunctionPtr agg_function, const Names & args, UInt64 row_count, DataTypePtr, PlanNodeStatisticsPtr & child_stats);
};

}
