#pragma once
#include <Query/Optimizer/Rule/Rule.h>

namespace DB
{
class PushAggThroughOuterJoin : public Rule
{
public:
  RuleType getType() const override { return RuleType::PUSH_AGG_THROUGH_OUTER_JOIN; }
  String getName() const override { return "PUSH_AGG_THROUGH_OUTER_JOIN"; }
  bool isEnabled(ContextPtr context) const override {return context->getOptimizerContext()->getSettingsRef().enable_push_agg_through_outer_join; }
  ConstRefPatternPtr getPattern() const override;

  TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

struct MappedAggregationInfo
{
  PlanNodePtr aggregation_node;
  std::unordered_map<String, String> symbol_mapping;
};


class PushAggThroughInnerJoin : public Rule
{
public:
  RuleType getType() const override { return RuleType::PUSH_AGG_THROUGH_INNER_JOIN; }
  String getName() const override { return "PUSH_AGG_THROUGH_INNER_JOIN"; }
  bool isEnabled(ContextPtr context) const override {return context->getOptimizerContext()->getSettingsRef().enable_push_agg_through_inner_join; }
  ConstRefPatternPtr getPattern() const override;

  TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
