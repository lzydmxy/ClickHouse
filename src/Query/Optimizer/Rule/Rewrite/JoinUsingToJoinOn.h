#pragma once

#include <Query/Optimizer/Rule/Rule.h>

namespace DB
{
class JoinUsingToJoinOn : public Rule
{
public:
    RuleType getType() const override { return RuleType::DISTINCT_TO_AGGREGATE; }
    String getName() const override { return "JOIN_USING_TO_JOIN_ON"; }
    bool isEnabled(ContextPtr context) const override
    {
        return context->getOptimizerContext()->getSettingsRef().enable_join_using_to_join_on;
    }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
