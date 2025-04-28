#pragma once
#include <Query/Optimizer/CostModel/PlanNodeCost.h>
#include <Query/Processors/QueryPlan/CTERefStepExt.h>

namespace DB
{
struct CostContext;

class CTECost
{
public:
    static PlanNodeCost calculate(const CTERefStepExt & step, CostContext & context);
};

}
