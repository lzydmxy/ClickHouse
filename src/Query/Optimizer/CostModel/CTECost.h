#pragma once
#include <Query/Optimizer/CostModel/PlanNodeCost.h>
#include <QueryPlan/CTERefStep.h>

namespace DB
{
struct CostContext;

class CTECost
{
public:
    static PlanNodeCost calculate(const CTERefStep & step, CostContext & context);
};

}
