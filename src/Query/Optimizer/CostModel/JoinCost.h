#pragma once
#include <Query/Optimizer/CostModel/PlanNodeCost.h>
#include <Query/Processors/QueryPlan/JoinStepExt.h>

namespace DB
{
struct CostContext;

class JoinCost
{
public:
    static PlanNodeCost calculate(const JoinStepExt & step, CostContext & context);
};

}
