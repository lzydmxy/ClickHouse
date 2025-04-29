#pragma once

#include <Interpreters/Context.h>
#include <Optimizer/Rewriter/Rewriter.h>
#include <Query/Processors/QueryPlan/QueryPlanExt.h>

namespace DB
{
class PlanOptimizer
{
public:
    static void optimize(QueryPlanExt & plan, ContextMutablePtr context);
    static void optimize(QueryPlanExt & plan, ContextMutablePtr context, const Rewriters & rewriters);
    static const Rewriters & getSimpleRewriters();
    static const Rewriters & getLegacyFullRewriters();
    static const Rewriters & getFullRewriters();
    static const Rewriters & getShortCircuitRewriters();
};

}
