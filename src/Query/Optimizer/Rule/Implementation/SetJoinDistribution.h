
#pragma once
#include <Query/Optimizer/Rule/Rule.h>

namespace DB
{
class SetJoinDistribution : public Rule
{
public:
    RuleType getType() const override { return RuleType::SET_JOIN_DISTRIBUTION; }
    String getName() const override { return "SET_JOIN_DISTRIBUTION"; }
    bool isEnabled(ContextPtr context) const override {return context->getSettingsRef().enable_set_join_distribution; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
