

#pragma once

#include <Query/Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Query/Processors/QueryPlan/AggregatingStepExt.h>
#include <Query/Processors/QueryPlan/DistinctStepExt.h>
#include <Query/Processors/QueryPlan/MergingAggregatedStepExt.h>

namespace DB
{
class AggregateEstimator
{
public:
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const AggregatingStepExt &, ContextMutablePtr context);
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const MergingAggregatedStepExt &, ContextMutablePtr context);
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const DistinctStepExt &, ContextMutablePtr context);

private:
    static SymbolStatisticsPtr estimateAggFun(AggregateFunctionPtr agg_function, const Names & args, UInt64 row_count, DataTypePtr, PlanNodeStatisticsPtr & child_stats);
};

}
