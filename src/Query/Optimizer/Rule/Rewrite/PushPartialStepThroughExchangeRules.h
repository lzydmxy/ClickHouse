#pragma once

#include <Query/Optimizer/Rule/Rule.h>

namespace DB
{
class PushPartialAggThroughExchange : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_PARTIAL_AGG_THROUGH_EXCHANGE; }
    String getName() const override { return "PUSH_PARTIAL_AGG_THROUGH_EXCHANGE"; }
    bool isEnabled(ContextPtr context) const override
    {
        return context->getOptimizerContext()->getSettingsRef().enable_push_partial_agg_through_exchange;
    }
    ConstRefPatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;

    static NameSet BLOCK_AGGS;
};

class PushProjectionThroughExchange: public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_PROJECTION_THROUGH_EXCHANGE; }
    String getName() const override { return "PUSH_PROJECTION_THROUGH_EXCHANGE"; }
    bool isEnabled(ContextPtr context) const override
    {
        return context->getOptimizerContext()->getSettingsRef().enable_push_projection_through_exchange;
    }
    ConstRefPatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class PushPartialAggThroughUnion : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_PARTIAL_AGG_THROUGH_UNION; }
    String getName() const override { return "PUSH_PARTIAL_AGG_THROUGH_UNION"; }
    bool isEnabled(ContextPtr context) const override
    {
        return context->getOptimizerContext()->getSettingsRef().enable_push_partial_agg_through_union;
    }
    ConstRefPatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class PushPartialSortingThroughExchange : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_PARTIAL_SORTING_THROUGH_EXCHANGE; }
    String getName() const override { return "PUSH_PARTIAL_SORTING_THROUGH_EXCHANGE"; }
    bool isEnabled(ContextPtr context) const override
    {
        return context->getOptimizerContext()->getSettingsRef().enable_push_partial_sorting_through_exchange;
    }
    ConstRefPatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class PushPartialSortingThroughUnion : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_PARTIAL_SORTING_THROUGH_UNION; }
    String getName() const override { return "PUSH_PARTIAL_SORTING_THROUGH_UNION"; }
    bool isEnabled(ContextPtr context) const override
    {
        return context->getOptimizerContext()->getSettingsRef().enable_push_partial_sorting_through_union;
    }
    ConstRefPatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class PushPartialLimitThroughExchange : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_PARTIAL_LIMIT_THROUGH_EXCHANGE; }
    String getName() const override { return "PUSH_PARTIAL_LIMIT_THROUGH_EXCHANGE"; }
    bool isEnabled(ContextPtr context) const override
    {
        return context->getOptimizerContext()->getSettingsRef().enable_push_partial_limit_through_exchange;
    }
    ConstRefPatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class PushPartialDistinctThroughExchange : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_PARTIAL_DISTINCT_THROUGH_EXCHANGE; }
    String getName() const override { return "PUSH_PARTIAL_DISTINCT_THROUGH_EXCHANGE"; }
    bool isEnabled(ContextPtr context) const override
    {
        return context->getOptimizerContext()->getSettingsRef().enable_push_partial_distinct_through_exchange;
    }
    ConstRefPatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class PushPartialTopNDistinctThroughExchange : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_PARTIAL_TOPN_DISTINCT_THROUGH_EXCHANGE; }
    String getName() const override { return "PUSH_PARTIAL_TOPN_DISTINCT_THROUGH_EXCHANGE"; }
    bool isEnabled(ContextPtr context) const override
    {
        return context->getOptimizerContext()->getSettingsRef().enable_push_partial_topn_distinct_through_exchange;
    }
    ConstRefPatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class MarkTopNDistinctThroughExchange : public Rule
{
public:
    RuleType getType() const override { return RuleType::MARK_TOPN_DISTINCT_THROUGH_EXCHANGE; }
    String getName() const override { return "MARK_TOPN_DISTINCT_THROUGH_EXCHANGE"; }
    bool isEnabled(ContextPtr context) const override
    {
        return context->getOptimizerContext()->getSettingsRef().enable_push_partial_topn_distinct_through_exchange;
    }
    ConstRefPatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
