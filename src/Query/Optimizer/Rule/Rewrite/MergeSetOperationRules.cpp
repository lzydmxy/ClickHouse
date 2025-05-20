#include <Query/Optimizer/Rule/Rewrite/MergeSetOperationRules.h>

#include <Query/Optimizer/MergeSetOperation.h>
#include <Query/Optimizer/Rule/Patterns.h>

namespace DB
{
ConstRefPatternPtr MergeUnionRule::getPattern() const
{
    static auto pattern = Patterns::unionn().result();
    return pattern;
}

TransformResult MergeUnionRule::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    SetOperationMerge merge_operation(node, *rule_context.context);
    auto result = merge_operation.merge();

    if (result)
        return result;
    else
        return {};
}

ConstRefPatternPtr MergeExceptRule::getPattern() const
{
    static auto pattern = Patterns::except().result();
    return pattern;
}

TransformResult MergeExceptRule::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto & context = *rule_context.context;
    if (!context.getOptimizerContext()->getSettingsRef().enable_setoperation_to_agg)
    {
        return {};
    }
    SetOperationMerge merge_operation(node, *rule_context.context);
    auto result = merge_operation.mergeFirstSource();

    if (result)
        return result;
    else
        return {};
}

ConstRefPatternPtr MergeIntersectRule::getPattern() const
{
    static auto pattern = Patterns::intersect().result();
    return pattern;
}

TransformResult MergeIntersectRule::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto & context = *rule_context.context;
    if (!context.getOptimizerContext()->getSettingsRef().enable_setoperation_to_agg)
    {
        return {};
    }
    SetOperationMerge merge_operation(node, *rule_context.context);
    auto result = merge_operation.merge();

    if (result)
        return result;
    else
        return {};
}
}
