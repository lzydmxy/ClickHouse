#pragma once

#include <Query/Optimizer/CostModel/PlanNodeCost.h>

namespace DB
{
struct CostContext;

class TableScanCost
{
public:
    static PlanNodeCost calculate(const TableScanStep & step, CostContext & context);
};

}
