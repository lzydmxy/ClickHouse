#pragma once
#include <Query/Optimizer/CostModel/PlanNodeCost.h>
#include <Query/Processors/QueryPlan/FilterStepExt.h>

namespace DB
{
struct CostContext;

class FilterCost
{
public:
    static PlanNodeCost calculate(const FilterStepExt & step, CostContext & context);
};

}
