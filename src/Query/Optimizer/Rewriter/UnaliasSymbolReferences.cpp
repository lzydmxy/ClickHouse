#include <Query/Optimizer/Rewriter/UnaliasSymbolReferences.h>

#include <Query/Planner/PlanSymbolReallocator.h>

namespace DB
{
bool UnaliasSymbolReferences::rewrite(QueryPlanExt & plan, ContextMutablePtr context) const
{
    auto rewrite = PlanSymbolReallocator::unalias(plan.getPlanNode(), context);
    plan.update(rewrite);
    for (const auto & cte : plan.getCTEInfo().getCTEs())
        plan.getCTEInfo().update(cte.first, PlanSymbolReallocator::unalias(cte.second, context));
    return true;
}
}
