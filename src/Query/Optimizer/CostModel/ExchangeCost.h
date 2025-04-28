#pragma once
#include <Query/Optimizer/CostModel/PlanNodeCost.h>
#include <Query/Processors/QueryPlan/ExchangeStepExt.h>

namespace DB
{
struct CostContext;

class ExchangeCost
{
public:
    static PlanNodeCost calculate(const ExchangeStepExt & node, CostContext & context);
};

}
