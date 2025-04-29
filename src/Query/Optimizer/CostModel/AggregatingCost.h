#pragma once
#include <Query/Optimizer/CostModel/PlanNodeCost.h>
#include <Query/Processors/QueryPlan/AggregatingStepExt.h>

namespace DB
{
struct CostContext;

class AggregatingCost
{
public:
    static PlanNodeCost calculate(const AggregatingStepExt & step, CostContext & context);
};

}
