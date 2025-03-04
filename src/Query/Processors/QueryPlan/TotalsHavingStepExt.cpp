#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/Transforms/TotalsHavingTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Query/Processors/QueryPlan/TotalsHavingStepExt.h>


namespace DB
{
TotalsHavingStepExt::TotalsHavingStepExt(
    const DataStream & input_stream_,
    const AggregateDescriptions & aggregates_,
    bool overflow_row_,
    const ConstASTPtr & having_filter_,
    const ActionsDAGPtr & actions_dag_,
    const std::string & filter_column_,
    bool remove_filter_,
    TotalsMode totals_mode_,
    double auto_include_threshold_,
    bool final_)
    : TotalsHavingStep(input_stream_, aggregates_, overflow_row_, actions_dag_, filter_column_, remove_filter_, totals_mode_, auto_include_threshold_, final_)
    , having_filter(having_filter_)
{
}

void TotalsHavingStepExt::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    if (!actions_dag && having_filter)
    {
        /*
        // todo: need to implement rewriteRuntimeFilter
        auto rewrite_filter = FilterStep::rewriteRuntimeFilter(having_filter, pipeline, settings);
        actions_dag = IQueryPlanStep::createFilterExpressionActions(
            settings.context, rewrite_filter->clone(), TotalsHavingTransform::transformHeader(input_streams[0].header, nullptr, final));
        filter_column_name = rewrite_filter->getColumnName();
        */
    }
    auto expression_actions = actions_dag ? std::make_shared<ExpressionActions>(actions_dag, settings.getActionsSettings()) : nullptr;

    auto totals_having = std::make_shared<TotalsHavingTransform>(
        pipeline.getHeader(),
        getAggregatesMask(pipeline.getHeader(), aggregates),
        overflow_row,
        expression_actions,
        filter_column_name,
        remove_filter,
        totals_mode,
        auto_include_threshold,
        final);

    pipeline.addTotalsHavingTransform(std::move(totals_having));

    if (!blocksHaveEqualStructure(pipeline.getHeader(), output_stream->header))
    {
        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            pipeline.getHeader().getColumnsWithTypeAndName(),
            output_stream->header.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name);
        auto convert_actions = std::make_shared<ExpressionActions>(convert_actions_dag, settings.getActionsSettings());

        pipeline.addSimpleTransform([&](const Block & header) { return std::make_shared<ExpressionTransform>(header, convert_actions); });
    }
}

std::shared_ptr<IQueryPlanStep> TotalsHavingStepExt::copy(ContextPtr) const
{
    return std::make_shared<TotalsHavingStepExt>(input_streams[0], aggregates, overflow_row, having_filter, actions_dag, filter_column_name, remove_filter, totals_mode, auto_include_threshold, final);
}

}
