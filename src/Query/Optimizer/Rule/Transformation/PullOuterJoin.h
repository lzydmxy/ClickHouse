
#pragma once
#include <Query/Optimizer/Rule/Rule.h>

namespace DB
{
class PullLeftJoinThroughInnerJoin : public Rule
{
public:
    RuleType getType() const override { return RuleType::PULL_LEFT_JOIN_THROUGH_INNER_JOIN; }
    String getName() const override { return "PULL_LEFT_JOIN_THROUGH_INNER_JOIN"; }
    bool isEnabled(ContextPtr context) const override {return context->getSettingsRef().enable_pull_outer_join; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};


class PullLeftJoinProjectionThroughInnerJoin : public Rule
{
public:
    RuleType getType() const override { return RuleType::PULL_LEFT_JOIN_PROJECTION_THROUGH_INNER_JOIN; }
    String getName() const override { return "PULL_LEFT_JOIN_PROJECTION_THROUGH_INNER_JOIN"; }
    bool isEnabled(ContextPtr context) const override {return context->getSettingsRef().enable_pull_outer_join; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class PullLeftJoinFilterThroughInnerJoin : public Rule
{
public:
    RuleType getType() const override { return RuleType::PULL_LEFT_JOIN_FILTER_THROUGH_INNER_JOIN; }
    String getName() const override { return "PULL_LEFT_JOIN_FILTER_THROUGH_INNER_JOIN"; }
    bool isEnabled(ContextPtr context) const override {return context->getSettingsRef().enable_pull_outer_join; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
