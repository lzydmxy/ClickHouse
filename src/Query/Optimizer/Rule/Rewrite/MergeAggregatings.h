#pragma once

#include <Query/Optimizer/Rule/Rule.h>
#include <Query/Optimizer/ExpressionRewriter.h>

namespace DB
{

class MergeAggregatings : public Rule
{
public:
    RuleType getType() const override { return RuleType::MERGE_AGGREGATINGS; }
    String getName() const override { return "MERGE_AGGREGATINGS"; }
    bool isEnabled(ContextPtr context) const override {return context->getOptimizerContext()->getSettingsRef().enable_merge_aggregate;}
    ConstRefPatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
