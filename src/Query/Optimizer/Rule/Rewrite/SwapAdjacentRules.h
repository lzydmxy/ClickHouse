#pragma once

#include <Query/Optimizer/Rule/Patterns.h>
#include <Query/Optimizer/Rule/Rule.h>

namespace DB
{

    class SwapAdjacentWindows : public Rule
    {
    public:
        RuleType getType() const override { return RuleType::SWAP_WINDOWS; }
        String getName() const override { return "SWAP_ADJACENT_WINDOWS"; }
        bool isEnabled(ContextPtr context) const override {return context->getOptimizerContext()->getSettingsRef().enable_windows_reorder; }
        ConstRefPatternPtr getPattern() const override { static auto pattern = Patterns::window().result(); return pattern; }

        TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
    };

}
