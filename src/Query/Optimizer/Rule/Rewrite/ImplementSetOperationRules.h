#pragma once

#include <Query/Optimizer/Rule/Rule.h>

namespace DB
{

class ImplementExceptRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::IMPLEMENT_EXCEPT; }
    String getName() const override { return "IMPLEMENT_EXCEPT"; }
    bool isEnabled(ContextPtr context) const override {return context->getOptimizerContext()->getSettingsRef().enable_implement_except; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class ImplementIntersectRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::IMPLEMENT_INTERSECT; }
    String getName() const override { return "IMPLEMENT_INTERSECT"; }
    bool isEnabled(ContextPtr context) const override {return context->getOptimizerContext()->getSettingsRef().enable_implement_intersect; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
