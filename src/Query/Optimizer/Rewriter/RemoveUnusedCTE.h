#pragma once

#include <Query/Optimizer/Rewriter/Rewriter.h>

namespace DB
{
/**
 * 1. Always Inlining Single-use CTEs
 * 2. Remove unused CTE from cte_info
 */
class RemoveUnusedCTE : public Rewriter
{
public:
    String name() const override { return "RemoveUnusedCTE"; }

private:
    bool rewrite(QueryPlanExt & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override { return context->getOptimizerContext()->getSettingsRef().enable_remove_unused_cte; }
    class Rewriter;
};
}
