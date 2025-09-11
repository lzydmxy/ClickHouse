#pragma once

#include <Query/Optimizer/Rule/Rule.h>

namespace DB
{

class RemoveFinalAggStep : public Rule
{
public:
    RuleType getType() const override { return RuleType::REMOVE_FINAL_AGG_STEP; }
    String getName() const override { return "REMOVE_FINAL_AGG_STEP"; }

    bool isEnabled(ContextPtr context) const override;

    ConstRefPatternPtr getPattern() const override;
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

/// TODO wujiancaho implement
class RemoveFinalDistinctStep
{
};

}
