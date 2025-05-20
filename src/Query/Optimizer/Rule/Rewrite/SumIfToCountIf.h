#pragma once

#include <Query/Optimizer/Rule/Rule.h>
#include <Query/Optimizer/SimpleExpressionRewriter.h>
#include <Query/Optimizer/ExpressionInterpreter.h>

namespace DB
{

    class SumIfToCountIf : public Rule
    {
    public:
        RuleType getType() const override { return RuleType::SUM_IF_TO_COUNT_IF; }
        String getName() const override { return "SumIfToCountIf"; }
        bool isEnabled(ContextPtr context) const override { return context->getOptimizerContext()->getSettingsRef().enable_sum_if_to_count_if; }
        ConstRefPatternPtr getPattern() const override;
        virtual bool excludeIfTransformSuccess() const override { return true; }
        virtual bool excludeIfTransformFailure() const override { return true; }

    protected:
        TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
    };

}
