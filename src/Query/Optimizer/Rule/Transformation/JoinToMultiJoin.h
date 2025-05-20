#pragma once

#include <Functions/FunctionsHashing.h>
#include <Query/Optimizer/Rule/Rule.h>
#include <Query/Processors/QueryPlan/JoinStepExt.h>

namespace DB
{
using GroupId = UInt32;
class CascadesContext;
class JoinToMultiJoin : public Rule
{
public:
    RuleType getType() const override { return RuleType::JOIN_TO_MULTI_JOIN; }
    String getName() const override { return "JOIN_TO_MULTI_JOIN"; }
    bool isEnabled(ContextPtr context) const override { return context->getOptimizerContext()->getSettingsRef().enable_join_to_multi_join; }
    ConstRefPatternPtr getPattern() const override;
    static bool isSupport(const JoinStepExt & s) { return s.supportReorder(true) && !s.isSimpleReordered() && !s.isOrdered(); }

    static PlanNodes createMultiJoin(
        ContextMutablePtr context,
        CascadesContext & optimizer_context,
        const JoinStepExt * join_step,
        GroupId group_id,
        GroupId left_group_id,
        GroupId right_group_id);

private:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};


}
