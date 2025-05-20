#pragma once

#include <Query/Optimizer/Rewriter/Rewriter.h>
#include <Query/Processors/QueryPlan/CTEInfo.h>
#include <Query/Processors/QueryPlan/CTERefStepExt.h>

namespace DB
{
/**
 * Unalias symbol references that are aliases of each other
 * Before:
 *   - Aggregate [$3, $5 = sum($4)]
 *     - Project[$3 = $0, $4 = $1 * $2]
 *       - TableScan [$0, $1, $2]
 *
 * After:
 *   - Aggregate [$0, $5 = sum($4)]
 *     - Project[$0, $4 = $1 * $2]
 *       - TableScan [$0, $1, $2]
 */
class UnaliasSymbolReferences : public Rewriter
{
public:
    String name() const override
    {
        return "UnaliasSymbolReferences";
    }

private:
    bool rewrite(QueryPlanExt & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override
    {
        return context->getOptimizerContext()->getSettingsRef().enable_unalias_symbol_references;
    }

    class Rewriter;
};
}
