#pragma once
#include <Query/Optimizer/Rule/Patterns.h>
#include <Query/Optimizer/Rule/Rule.h>

namespace DB
{
class FilterWindowToPartitionTopN : public Rule
{
public:
    RuleType getType() const override { return RuleType::FILTER_WINDOW_TO_PARTITION_TOPN; }
    String getName() const override { return "FILTER_WINDOW_TO_PARTITION_TOPN"; }
    bool isEnabled(ContextPtr context) const override {return context->getOptimizerContext()->getSettingsRef().enable_filter_window_to_partition_topn;; }
    ConstRefPatternPtr getPattern() const override { static auto pattern = Patterns::filter().withSingle(Patterns::window().withSingle(Patterns::exchange())).result(); return pattern;}

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
