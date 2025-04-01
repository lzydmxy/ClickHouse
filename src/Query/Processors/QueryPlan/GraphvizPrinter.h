#pragma once

#include <Query/Processors/QueryPlan/PlanNode.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/Executor/PlanSegment.h>

namespace DB
{

class GraphvizPrinter
{
//toto: need to impl, now just a fake impl for build, StepPrinter,PlanSegmentEdgePrinter,PlanSegmentNodePrinter,PlanNodeEdgePrinter,PlanNodePrinter also need impl
public:
    static void printLogicalPlan(PlanNodeBase &, ContextMutablePtr &, const String & name) {}
    static void printLogicalPlan(QueryPlanExt &, ContextMutablePtr &, const String & name, StepProfiles profiles = {}) {}
    static void printPlanSegment(const PlanSegmentTreeUniqPtr &, const ContextMutablePtr &) {}
};

}
