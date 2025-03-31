#pragma once

#include <Interpreters/Context.h>
#include <Query/Processors/QueryPlan/PlanNode.h>
//todo: need Rewriter
//#include <Optimizer/Rewriter/Rewriter.h>

namespace DB
{
class PlanOptimizer
{
public:
    //todo: need impl the optimize, now just a empty impl
    static void optimize(QueryPlan & plan, ContextMutablePtr context) {}
    //static void optimize(QueryPlan & plan, ContextMutablePtr context, const Rewriters & rewriters);
    //static const Rewriters & getSimpleRewriters();
    //static const Rewriters & getLegacyFullRewriters();
    //static const Rewriters & getFullRewriters();
    //static const Rewriters & getShortCircuitRewriters();
};

}
