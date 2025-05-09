#pragma once

#include <Query/Optimizer/Rule/Rule.h>

namespace DB
{
class DistinctToAggregate : public Rule
{
public:
    RuleType getType() const override { return RuleType::DISTINCT_TO_AGGREGATE; }
    String getName() const override { return "DISTINCT_TO_AGGREGATE"; }
    bool isEnabled(ContextPtr context) const override {return context->getOptimizerContext()->getSettingsRef().enable_distinct_to_aggregate; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
