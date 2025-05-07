#include <Query/Optimizer/Rule/Transformation/InlineCTE.h>

#include <Query/Optimizer/Cascades/CascadesOptimizer.h>
#include <Query/Optimizer/Iterative/IterativeRewriter.h>
#include <Query/Optimizer/Rewriter/ColumnPruning.h>
#include <Query/Optimizer/Rewriter/PredicatePushdown.h>
#include <Query/Optimizer/Rewriter/UnifyJoinOutputs.h>
#include <Query/Optimizer/Rule/Patterns.h>
#include <Query/Optimizer/Rule/Rules.h>
#include <Query/Processors/QueryPlan/CTEInfo.h>
#include <Query/Processors/QueryPlan/CTERefStepExt.h>
#include <Query/Planner/GraphvizPrinter.h>
#include <Query/Processors/QueryPlan/PlanNode.h>

namespace DB
{
ConstRefPatternPtr InlineCTE::getPattern() const
{
    static auto pattern = Patterns::cte().result();
    return pattern;
}

TransformResult InlineCTE::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto * cte_step = dynamic_cast<const CTERefStepExt *>(node->getStep().get());
    if (cte_step->hasFilter())
        return {}; // InlineCTEWithFilter

    auto inlined_plan = cte_step->toInlinedPlanNode(context.cte_info, context.context);
    return InlineCTE::reoptimize(cte_step->getId(), inlined_plan, context.cte_info, context.context);
}

ConstRefPatternPtr InlineCTEWithFilter::getPattern() const
{
    static auto pattern = Patterns::filter().withSingle(Patterns::cte()).result();
    return pattern;
}

TransformResult InlineCTEWithFilter::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    auto cte = node->getChildren()[0];
    const auto * cte_step = dynamic_cast<const CTERefStepExt *>(cte->getStep().get());
    if (!cte_step->hasFilter())
        return {}; // InlineCTE

    auto inlined_plan
        = PlanNodeBase::createPlanNode(node->getId(), node->getStep(), {cte_step->toInlinedPlanNode(context.cte_info, context.context)});
    return InlineCTE::reoptimize(cte_step->getId(), inlined_plan, context.cte_info, context.context);
}

PlanNodePtr InlineCTE::reoptimize(CTEId cte_id, const PlanNodePtr & node, CTEInfo & cte_info, ContextMutablePtr & context)
{
    if (context->getOptimizerContext()->getSettingsRef().print_graphviz)
        GraphvizPrinter::printLogicalPlan(
            *node, context, fmt::format("{}_cte_{}__inlined", context->getOptimizerContext()->getRuleId(), cte_id));

    static Rewriters rewriters
        = {std::make_shared<ColumnPruning>(),
           std::make_shared<PredicatePushdown>(false, true),
           std::make_shared<IterativeRewriter>(Rules::inlineProjectionRules(), "InlineProjection"),
           std::make_shared<IterativeRewriter>(Rules::normalizeExpressionRules(), "NormalizeExpression"),
           std::make_shared<IterativeRewriter>(Rules::swapPredicateRules(), "SwapPredicate"),
           std::make_shared<IterativeRewriter>(Rules::simplifyExpressionRules(), "SimplifyExpression"),
           std::make_shared<IterativeRewriter>(Rules::removeRedundantRules(), "RemoveRedundant"),
           std::make_shared<UnifyJoinOutputs>()};

    QueryPlanExt sub_plan{node, cte_info, context->getOptimizerContext()->getPlanNodeIdAllocator()};
    for (auto & rewriter : rewriters)
        rewriter->rewritePlan(sub_plan, context);
    return sub_plan.getPlanNode();
}
}
