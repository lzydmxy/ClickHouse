#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Query/Processors/QueryPlan/ExceptStepExt.h>

namespace DB
{

ExceptStepExt::ExceptStepExt(
    DataStreams input_streams_,
    DataStream output_stream_,
    std::unordered_map<String, std::vector<String>> output_to_inputs_,
    bool distinct_)
    : SetOperationStepExt(input_streams_, output_stream_, output_to_inputs_), distinct(distinct_)
{
}

QueryPipelineBuilderPtr ExceptStepExt::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & context)
{
    (void)pipelines;
    (void)context;
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "except step is not implemented");
#if 0
    auto pipeline = std::make_unique<QueryPipeline>();
    QueryPipelineProcessorsCollector collector(*pipeline, this);

    if (pipelines.empty())
    {
        pipeline->init(Pipe(std::make_shared<NullSource>(output_stream->header)));
        processors = collector.detachProcessors();
        return pipeline;
    }

    for (auto & cur_pipeline : pipelines)
    {
        /// Just in case.
        if (!isCompatibleHeader(cur_pipeline->getHeader(), getOutputStream().header))
        {
            auto converting_dag = ActionsDAG::makeConvertingActions(
                cur_pipeline->getHeader().getColumnsWithTypeAndName(),
                getOutputStream().header.getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Position);

            auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));
            cur_pipeline->addSimpleTransform(
                [&](const Block & cur_header) { return std::make_shared<ExpressionTransform>(cur_header, converting_actions); });
        }

        /// For the case of union.
        cur_pipeline->addTransform(std::make_shared<ResizeProcessor>(getOutputStream().header, cur_pipeline->getNumStreams(), 1));
    }

    *pipeline = QueryPipeline::unitePipelines(std::move(pipelines), context.context->getSettingsRef().max_threads);
    pipeline->addTransform(std::make_shared<IntersectOrExceptTransform>(
        getOutputStream().header,
        distinct ? ASTSelectIntersectExceptQuery::Operator::EXCEPT_DISTINCT: ASTSelectIntersectExceptQuery::Operator::EXCEPT_ALL));

    processors = collector.detachProcessors();
    return pipeline;
#endif
}

void ExceptStepExt::toProto(Protos::ExceptStepExt & proto, bool) const
{
    SetOperationStepExt::serializeToProtoBase(*proto.mutable_query_plan_base());
    proto.set_distinct(distinct);
}

std::shared_ptr<ExceptStepExt> ExceptStepExt::fromProto(const Protos::ExceptStepExt & proto, ContextPtr)
{
    auto [base_input_streams, base_output_stream, output_to_inputs]
        = SetOperationStepExt::deserializeFromProtoBase(proto.query_plan_base());
    auto distinct = proto.distinct();
    auto step = std::make_shared<ExceptStepExt>(base_input_streams, base_output_stream, output_to_inputs, distinct);

    return step;
}

bool ExceptStepExt::isDistinct() const
{
    return distinct;
}

std::shared_ptr<IQueryPlanStep> ExceptStepExt::copy(ContextPtr) const
{
    return std::make_unique<ExceptStepExt>(input_streams, output_stream.value(), distinct);
}

}
