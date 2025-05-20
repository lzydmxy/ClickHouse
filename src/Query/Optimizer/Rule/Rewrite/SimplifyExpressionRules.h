#pragma once

#include <Query/Optimizer/Rule/Rule.h>
#include <Query/Optimizer/DomainTranslator.h>

namespace DB {

class CommonPredicateRewriteRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::COMMON_PREDICATE_REWRITE; }
    String getName() const override { return "COMMON_PREDICATE_REWRITE"; }
    bool isEnabled(ContextPtr context) const override {return context->getOptimizerContext()->getSettingsRef().enable_common_predicate_rewrite; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class CommonJoinFilterRewriteRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::COMMON_JOIN_FILTER_REWRITE; }
    String getName() const override { return "COMMON_JOIN_FILTER_REWRITE"; }
    bool isEnabled(ContextPtr context) const override {return context->getOptimizerContext()->getSettingsRef().enable_common_join_predicate_rewrite; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class SwapPredicateRewriteRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::SWAP_PREDICATE_REWRITE; }
    String getName() const override { return "SWAP_PREDICATE_REWRITE"; }
    bool isEnabled(ContextPtr context) const override {return context->getOptimizerContext()->getSettingsRef().enable_swap_predicate_rewrite; }
    bool excludeIfTransformSuccess() const override { return true; }
    bool excludeIfTransformFailure() const override { return true; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class SimplifyPredicateRewriteRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::SIMPLIFY_PREDICATE_REWRITE; }
    String getName() const override { return "SIMPLIFY_PREDICATE_REWRITE"; }
    bool isEnabled(ContextPtr context) const override {return context->getOptimizerContext()->getSettingsRef().enable_simplify_predicate_rewrite; }
    bool excludeIfTransformSuccess() const override { return true; }
    bool excludeIfTransformFailure() const override { return true; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};


class UnWarpCastInPredicateRewriteRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::UN_WARP_CAST_IN_PREDICATE_REWRITE; }
    String getName() const override { return "UN_WARP_CAST_IN_PREDICATE_REWRITE"; }
    bool isEnabled(ContextPtr context) const override {return context->getOptimizerContext()->getSettingsRef().enable_unwrap_cast_in; }
    bool excludeIfTransformSuccess() const override { return true; }
    bool excludeIfTransformFailure() const override { return true; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class SimplifyJoinFilterRewriteRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::SIMPLIFY_JOIN_FILTER_REWRITE; }
    String getName() const override { return "SIMPLIFY_JOIN_FILTER_REWRITE"; }
    bool isEnabled(ContextPtr context) const override {return context->getOptimizerContext()->getSettingsRef().enable_simplify_join_filter_rewrite; }
    bool excludeIfTransformSuccess() const override { return true; }
    bool excludeIfTransformFailure() const override { return true; }

    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class SimplifyExpressionRewriteRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::SIMPLIFY_EXPRESSION_REWRITE; }
    String getName() const override { return "SIMPLIFY_EXPRESSION_REWRITE"; }
    bool isEnabled(ContextPtr context) const override {return context->getOptimizerContext()->getSettingsRef().enable_simplify_expression_rewrite; }
    bool excludeIfTransformSuccess() const override { return true; }
    bool excludeIfTransformFailure() const override { return true; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class MergePredicatesUsingDomainTranslator : public Rule
{
public:
    RuleType getType() const override { return RuleType::MERGE_PREDICATES_USING_DOMAIN_TRANSLATOR; }
    String getName() const override { return "MERGE_PREDICATES_USING_DOMAIN_TRANSLATOR"; }
    bool isEnabled(ContextPtr context) const override {return context->getOptimizerContext()->getSettingsRef().rewrite_predicate_by_domain; }
    bool excludeIfTransformSuccess() const override { return true; }
    bool excludeIfTransformFailure() const override { return true; }

    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}

