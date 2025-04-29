#pragma once

#include <Interpreters/Context.h>
#include <Query/Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Query/Processors/QueryPlan/TableScanStepExt.h>

namespace DB
{
class TableScanEstimator
{
public:
    static PlanNodeStatisticsPtr estimate(ContextMutablePtr context, const TableScanStepExt & step);
    static std::optional<PlanNodeStatisticsPtr> estimate(
        ContextMutablePtr context, const StorageID & storage_id, const Names & columns = {});
};

}
