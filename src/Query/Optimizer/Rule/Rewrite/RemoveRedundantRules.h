#pragma once

#include <Query/Optimizer/Rule/Patterns.h>
#include <Query/Optimizer/Rule/Rule.h>

namespace DB
{
class RemoveRedundantFilter : public Rule
{
public:
    RuleType getType() const override { return RuleType::REMOVE_REDUNDANT_FILTER; }
    String getName() const override { return "REMOVE_REDUNDANT_FILTER"; }
    bool isEnabled(ContextPtr context) const override { return context->getOptimizerContext()->getSettingsRef().enable_remove_redundant; }
    ConstRefPatternPtr getPattern() const override { static auto pattern = Patterns::filter().result(); return pattern; }

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class RemoveRedundantUnion : public Rule
{
public:
    RuleType getType() const override { return RuleType::REMOVE_REDUNDANT_UNION; }
    String getName() const override { return "REMOVE_REDUNDANT_UNION"; }
    bool isEnabled(ContextPtr context) const override { return context->getOptimizerContext()->getSettingsRef().enable_remove_redundant; }
    ConstRefPatternPtr getPattern() const override { static auto pattern = Patterns::unionn().result(); return pattern; }

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class RemoveRedundantProjection : public Rule
{
public:
    RuleType getType() const override { return RuleType::REMOVE_REDUNDANT_PROJECTION; }
    String getName() const override { return "REMOVE_REDUNDANT_PROJECTION"; }
    bool isEnabled(ContextPtr context) const override { return context->getOptimizerContext()->getSettingsRef().enable_remove_redundant; }
    ConstRefPatternPtr getPattern() const override { static auto pattern = Patterns::project().result(); return pattern; }

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class RemoveRedundantEnforceSingleRow : public Rule
{
public:
    RuleType getType() const override { return RuleType::REMOVE_REDUNDANT_ENFORCE_SINGLE_ROW; }
    String getName() const override { return "REMOVE_REDUNDANT_ENFORCE_SINGLE_ROW"; }
    bool isEnabled(ContextPtr context) const override { return context->getOptimizerContext()->getSettingsRef().enable_remove_redundant; }
    ConstRefPatternPtr getPattern() const override { static auto pattern = Patterns::enforceSingleRow().result(); return pattern; }

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class RemoveRedundantCrossJoin : public Rule
{
public:
    RuleType getType() const override { return RuleType::REMOVE_REDUNDANT_CROSS_JOIN; }
    String getName() const override { return "REMOVE_REDUNDANT_CROSS_JOIN"; }
    bool isEnabled(ContextPtr context) const override { return context->getOptimizerContext()->getSettingsRef().enable_remove_redundant; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class RemoveReadNothing : public Rule
{
public:
    RuleType getType() const override { return RuleType::REMOVE_READ_NOTHING; }
    String getName() const override { return "REMOVE_READ_NOTHING"; }
    bool isEnabled(ContextPtr context) const override { return context->getOptimizerContext()->getSettingsRef().enable_remove_redundant; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class RemoveRedundantJoin : public Rule
{
public:
    RuleType getType() const override { return RuleType::REMOVE_REDUNDANT_JOIN; }
    String getName() const override { return "REMOVE_REDUNDANT_JOIN"; }
    bool isEnabled(ContextPtr context) const override { return context->getOptimizerContext()->getSettingsRef().enable_remove_redundant; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class RemoveRedundantOuterJoin : public Rule
{
public:
    RuleType getType() const override { return RuleType::REMOVE_REDUNDANT_OUTER_JOIN; }
    String getName() const override { return "REMOVE_REDUNDANT_JOIN"; }
    bool isEnabled(ContextPtr context) const override { return context->getOptimizerContext()->getSettingsRef().enable_remove_redundant; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};


class RemoveRedundantLimit : public Rule
{
public:
    RuleType getType() const override { return RuleType::REMOVE_REDUNDANT_LIMIT; }
    String getName() const override { return "REMOVE_REDUNDANT_LIMIT"; }
    bool isEnabled(ContextPtr context) const override { return context->getOptimizerContext()->getSettingsRef().enable_remove_redundant; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

// todo: bc, group by columns is distinct, no aggregate functions.
class RemoveRedundantAggregate : public Rule
{
public:
    RuleType getType() const override { return RuleType::REMOVE_REDUNDANT_AGGREGATE; }
    String getName() const override { return "REMOVE_REDUNDANT_AGGREGATE"; }
    bool isEnabled(ContextPtr context) const override { return context->getOptimizerContext()->getSettingsRef().enable_remove_redundant; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class RemoveRedundantAggregateWithReadNothing : public Rule
{
public:
    RuleType getType() const override { return RuleType::REMOVE_REDUNDANT_AGGREGATE_WITH_READ_NOTHING; }
    String getName() const override { return "REMOVE_REDUNDANT_AGGREGATE_WITH_READ_NOTHING"; }
    bool isEnabled(ContextPtr context) const override { return context->getOptimizerContext()->getSettingsRef().enable_remove_redundant; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class RemoveRedundantTwoApply : public Rule
{
public:
    RuleType getType() const override { return RuleType::REMOVE_REDUNDANT_TWO_APPLY; }
    String getName() const override { return "REMOVE_REDUNDANT_TWO_APPLY"; }
    bool isEnabled(ContextPtr context) const override { return context->getOptimizerContext()->getSettingsRef().enable_remove_redundant; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
