#pragma once
#include <Query/Optimizer/CostModel/PlanNodeCost.h>
#include <Query/Processors/QueryPlan/ProjectionStepExt.h>

namespace DB
{
struct CostContext;

class ProjectionCost
{
public:
    static PlanNodeCost calculate(const ProjectionStepExt & step, CostContext & context);
};

}
