#pragma once

#include <Query/Optimizer/Rewriter/Rewriter.h>
#include <Query/Optimizer/Rule/Rule.h>
#include <QueryPlan/CTEInfo.h>

namespace DB
{
class InlineCTE : public Rule
{
public:
    RuleType getType() const override { return RuleType::INLINE_CTE; }
    String getName() const override { return "INLINE_CTE"; }
    bool isEnabled(ContextPtr context) const override { return context->getSettingsRef().cte_mode == CTEMode::AUTO; }
    ConstRefPatternPtr getPattern() const override;

    /**
     * In order for cascades to calculate the right cost, some ruls need be applied for inlined plan, 
     * like PredicatePushDown, SimplifyExpression, RemoveRedundant and so on.
     */
    static PlanNodePtr reoptimize(CTEId cte_id, const PlanNodePtr & node, CTEInfo & cte_info, ContextMutablePtr & context);

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class InlineCTEWithFilter : public Rule
{
public:
    RuleType getType() const override { return RuleType::INLINE_CTE_WITH_FILTER; }
    String getName() const override { return "InlineCTEWithFilter"; }
    bool isEnabled(ContextPtr context) const override { return context->getSettingsRef().cte_mode == CTEMode::AUTO; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};
}
