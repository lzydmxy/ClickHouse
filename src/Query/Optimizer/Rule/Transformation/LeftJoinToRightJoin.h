#pragma once

#include <Query/Optimizer/Rule/Rule.h>
#include <Query/Processors/QueryPlan/JoinStepExt.h>

namespace DB
{
class LeftJoinToRightJoin : public Rule
{
public:
    RuleType getType() const override { return RuleType::LEFT_JOIN_TO_RIGHT_JOIN; }
    String getName() const override { return "LEFT_JOIN_TO_RIGHT_JOIN"; }
    bool isEnabled(ContextPtr context) const override {return context->getOptimizerContext()->getSettingsRef().enable_left_join_to_right_join; }
    ConstRefPatternPtr getPattern() const override;

    // Left join with filter is not allowed convert to Right join with filter. (nest loop join only support left join).
    static bool supportSwap(const JoinStepExt & s)
    {
        return (s.getKind() == JoinKind::Left || s.getKind() == JoinKind::Full) && s.supportSwap();
    }

    const std::vector<RuleType> & blockRules() const override
    {
        static std::vector<RuleType> block{RuleType::LEFT_JOIN_TO_RIGHT_JOIN};
        return block;
    }

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
