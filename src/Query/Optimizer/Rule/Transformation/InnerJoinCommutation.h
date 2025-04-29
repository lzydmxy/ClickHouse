#pragma once

#include <Query/Optimizer/Rule/Rule.h>
#include <Query/Processors/QueryPlan/JoinStepExt.h>

namespace DB
{
/**
 * InnerJoinCommutation is mutually exclusive with JoinEnumOnGraph rule,
 * the later also do inner join commutation works.
 *
 * Transforms:
 * <pre>
 * - Inner Join
 *     - X
 *     - Y
 * </pre>
 * Into:
 * <pre>
 * - Inner Join
 *     - Y
 *     - X
 * </pre>
 */
class InnerJoinCommutation : public Rule
{
public:
    RuleType getType() const override { return RuleType::INNER_JOIN_COMMUTATION; }
    String getName() const override { return "INNER_JOIN_COMMUTATION"; }
    bool isEnabled(ContextPtr context) const override {return context->getOptimizerContext()->getSettingsRef().enable_inner_join_commutation; }
    ConstRefPatternPtr getPattern() const override;

    const std::vector<RuleType> & blockRules() const override;

    static bool supportSwap(const JoinStepExt & s) { return s.getKind() == JoinKind::Inner && s.supportSwap(); }
    static PlanNodePtr swap(JoinStepExtNode & node, RuleContext & rule_context);

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
