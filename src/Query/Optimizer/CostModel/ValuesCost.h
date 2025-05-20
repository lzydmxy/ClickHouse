#pragma once
#include <Query/Optimizer/CostModel/PlanNodeCost.h>
#include <Query/Processors/QueryPlan/ValuesStepExt.h>

namespace DB
{
struct CostContext;

class ValuesCost
{
public:
    static PlanNodeCost calculate(const ValuesStepExt & step, CostContext & context);
};

}
