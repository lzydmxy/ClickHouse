#pragma once

#include <Query/Optimizer/Rule/Rule.h>

namespace DB
{
class MergeUnionRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::MERGE_UNION; }
    String getName() const override { return "MERGE_UNION"; }
    bool isEnabled(ContextPtr context) const override {return context->getOptimizerContext()->getSettingsRef().enable_merge_union; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class MergeExceptRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::MERGE_EXCEPT; }
    String getName() const override { return "MERGE_EXCEPT"; }
    bool isEnabled(ContextPtr context) const override {return context->getOptimizerContext()->getSettingsRef().enable_merge_except; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class MergeIntersectRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::MERGE_INTERSECT; }
    String getName() const override { return "MERGE_INTERSECT"; }
    bool isEnabled(ContextPtr context) const override {return context->getOptimizerContext()->getSettingsRef().enable_merge_intersect; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
