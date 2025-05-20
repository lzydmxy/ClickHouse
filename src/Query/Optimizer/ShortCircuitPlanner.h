#pragma once

#include <Interpreters/Context_fwd.h>
#include <Query/Processors/QueryPlan/QueryPlanExt.h>

namespace DB
{
class ShortCircuitPlanner
{
public:
    static bool isShortCircuitPlan(QueryPlanExt & query_plan, ContextPtr context);
    static void addExchangeIfNeeded(QueryPlanExt & query_plan, ContextMutablePtr context);

private:
    class ShortCircuitPlanVisitor;
};
}
