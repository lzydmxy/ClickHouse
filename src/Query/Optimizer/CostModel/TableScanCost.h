#pragma once

#include <Query/Optimizer/CostModel/PlanNodeCost.h>
#include <Query/Processors/QueryPlan/TableScanStepExt.h>

namespace DB
{
struct CostContext;

class TableScanCost
{
public:
    static PlanNodeCost calculate(const TableScanStepExt & step, CostContext & context);
};

}
