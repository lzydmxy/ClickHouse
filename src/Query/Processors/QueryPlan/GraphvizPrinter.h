#pragma once

#include <Query/Processors/QueryPlan/PlanNode.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/Executor/PlanSegment.h>

namespace DB
{

class GraphvizPrinter
{
//toto: need to impl
public:
    static void printLogicalPlan(PlanNodeBase &, ContextMutablePtr &, const String & name) {}
    static void printLogicalPlan(QueryPlanExt &, ContextMutablePtr &, const String & name, StepProfiles profiles = {}) {}
    static void printPlanSegment(const PlanSegmentTreeUniqPtr &, const ContextMutablePtr &) {}
};

}
