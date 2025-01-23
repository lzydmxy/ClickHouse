#pragma once
#include <Query/Optimizer/CostModel/PlanNodeCost.h>
#include <QueryPlan/JoinStep.h>

namespace DB
{
struct CostContext;

class JoinCost
{
public:
    static PlanNodeCost calculate(const JoinStep & step, CostContext & context);
};

}
