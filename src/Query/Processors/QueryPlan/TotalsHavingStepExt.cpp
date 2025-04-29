#include <Interpreters/ExpressionActions.h>
#include <Processors/Transforms/TotalsHavingTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Query/Processors/QueryPlan/TotalsHavingStepExt.h>
#include <Query/Processors/QueryPlan/FilterStepExt.h>
#include <Query/Processors/QueryPlan/QueryPlanStepHelper.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>

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
        auto rewrite_filter = FilterStepExt::rewriteRuntimeFilter(having_filter, pipeline, settings);
        actions_dag = QueryPlanStepHelper::createFilterExpressionActions(settings.getBuildQueryPipelineSettingsExt().context,
                                                                         rewrite_filter->clone(),
                                                                         TotalsHavingTransform::transformHeader(input_streams[0].header, actions_dag.get(), filter_column_name, remove_filter, final, getAggregatesMask(input_streams[0].header, aggregates)));
        filter_column_name = rewrite_filter->getColumnName();
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

void TotalsHavingStepExt::toProto(Protos::TotalsHavingStepExt & proto, bool for_hash_equals) const
{
    if (actions_dag)
    {
        throw Exception(ErrorCodes::PROTOBUF_BAD_CAST, "actions dag is not supported in protobuf");
    }

    ProtosSerDerHelper::serializeToProtoBase(*this, *proto.mutable_query_plan_base());
    if (having_filter)
        serializeASTToProto(having_filter, *proto.mutable_having_filter());
    proto.set_overflow_row(overflow_row);
    proto.set_filter_column_name(filter_column_name);
    //todo:liyang453, other feat: need TotalsModeConverter
    //proto.set_totals_mode(TotalsModeConverter::toProto(totals_mode));
    proto.set_auto_include_threshold(auto_include_threshold);
    proto.set_final(final);
}

std::shared_ptr<TotalsHavingStepExt> TotalsHavingStepExt::fromProto(const Protos::TotalsHavingStepExt & proto, ContextPtr)
{
    auto [step_description, base_input_stream] = ProtosSerDerHelper::deserializeFromProtoBase(proto.query_plan_base());
    auto having_filter = proto.has_having_filter() ? deserializeASTFromProto(proto.having_filter()): nullptr;
    //todo:liyang453, other feat: need TotalsModeConverter
    //auto totals_mode = TotalsModeConverter::fromProto(proto.totals_mode());
    AggregateDescriptions aggregates;
    TotalsMode totals_mode;
    auto step = std::make_shared<TotalsHavingStepExt>(base_input_stream, aggregates, proto.overflow_row(), having_filter, nullptr, proto.filter_column_name(), false, totals_mode, proto.auto_include_threshold(), proto.final());
    step->setStepDescription(step_description);
    return step;
}

}
