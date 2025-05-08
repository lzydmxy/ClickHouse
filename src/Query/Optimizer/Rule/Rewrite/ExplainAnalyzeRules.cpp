#include <Query/Optimizer/Rule/Rewrite/ExplainAnalyzeRules.h>
#include <Query/Optimizer/Rule/Patterns.h>

#include <Query/Optimizer/CardinalityEstimate/CardinalityEstimator.h>
#include <Query/Processors/QueryPlan/ExplainAnalyzeStepExt.h>

namespace DB
{

ConstRefPatternPtr ExplainAnalyze::getPattern() const
{
    static auto pattern = Patterns::explainAnalyze().matchingStep<ExplainAnalyzeStepExt>([](const auto & step) { return !step.hasPlan(); }).result();
    return pattern;
}

TransformResult ExplainAnalyze::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto original_query_plan_ptr = std::make_shared<QueryPlanExt>(node->getChildren()[0], rule_context.cte_info, rule_context.context->getOptimizerContext()->getPlanNodeIdAllocator());
    CardinalityEstimator::estimate(*original_query_plan_ptr, rule_context.context, true);

    const auto & explain_step = dynamic_cast<const ExplainAnalyzeStepExt &>(*node->getStep());
    auto new_explain_analyze_step = std::make_shared<ExplainAnalyzeStepExt>(
        explain_step.getInputStreams()[0],
        explain_step.getOutputName(),
        explain_step.getKind(),
        rule_context.context,
        original_query_plan_ptr,
        explain_step.getSetting());

    return PlanNodeBase::createPlanNode(
        rule_context.context->getOptimizerContext()->nextNodeId(),
        new_explain_analyze_step,
        node->getChildren(),
        node->getStatistics()
    );
}

}
