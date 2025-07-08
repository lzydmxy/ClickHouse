#include <Query/Optimizer/Rewriter/Rewriter.h>
#include <Query/Planner/GraphvizPrinter.h>


namespace DB
{
void Rewriter::rewritePlan(QueryPlanExt & plan, ContextMutablePtr context) const
{
    Stopwatch watch{CLOCK_THREAD_CPUTIME_ID};
    watch.restart();

    bool rewritten = false;
    if (isEnabled(context))
    {
        rewritten = rewrite(plan, context);
    }

    const auto & optimizer_context =  context->getOptimizerContext();

    auto duration = watch.elapsedMilliseconds();

    if (duration > 1)
        LOG_DEBUG(log, "Optimizer rule {} run time: {} ms which is larget than 1ms", name(), duration);

    if (duration >= optimizer_context->getSettingsRef().plan_optimizer_rule_warning_time)
        LOG_WARNING(log,
            "the execute time of {} rewriter {} ms greater than or equal to {} ms",
            name(), duration, optimizer_context->getSettingsRef().plan_optimizer_rule_warning_time);

    if (optimizer_context->getSettingsRef().print_graphviz)
    {
        GraphvizPrinter::printLogicalPlan(
            plan, context, fmt::format("{}_{}_{}ms", optimizer_context->getRuleId(), name(), std::to_string(duration)));
    }

    if (rewritten)
    {
        for (const auto & after_rule : after_rules)
        {
            optimizer_context->incRuleId();
            after_rule->rewritePlan(plan, context);
        }
    }
}

}
