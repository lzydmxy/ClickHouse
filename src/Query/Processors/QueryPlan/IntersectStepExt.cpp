#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Query/Processors/QueryPlan/IntersectStepExt.h>
// #include <Processors/Transforms/IntersectOrExceptTransform.h>

namespace DB
{

IntersectStepExt::IntersectStepExt(
    DataStreams input_streams_,
    DataStream output_stream_,
    std::unordered_map<String, std::vector<String>> output_to_inputs_,
    bool distinct_)
    : SetOperationStepExt(input_streams_, output_stream_, output_to_inputs_), distinct(distinct_)
{
}

QueryPipelineBuilderPtr IntersectStepExt::updatePipeline(QueryPipelineBuilders, const BuildQueryPipelineSettings &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "intersect step is not implemented");
    // auto pipeline = std::make_unique<QueryPipeline>();
    // QueryPipelineProcessorsCollector collector(*pipeline, this);

    // if (pipelines.empty())
    // {
    //     pipeline->init(Pipe(std::make_shared<NullSource>(output_stream->header)));
    //     processors = collector.detachProcessors();
    //     return pipeline;
    // }

    // for (auto & cur_pipeline : pipelines)
    // {
    //     /// Just in case.
    //     if (!isCompatibleHeader(cur_pipeline->getHeader(), getOutputStream().header))
    //     {
    //         auto converting_dag = ActionsDAG::makeConvertingActions(
    //             cur_pipeline->getHeader().getColumnsWithTypeAndName(),
    //             getOutputStream().header.getColumnsWithTypeAndName(),
    //             ActionsDAG::MatchColumnsMode::Position);

    //         auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));
    //         cur_pipeline->addSimpleTransform(
    //             [&](const Block & cur_header) { return std::make_shared<ExpressionTransform>(cur_header, converting_actions); });
    //     }

    //     /// For the case of union.
    //     cur_pipeline->addTransform(std::make_shared<ResizeProcessor>(getOutputStream().header, cur_pipeline->getNumStreams(), 1));
    // }

    // *pipeline = QueryPipeline::unitePipelines(std::move(pipelines), context.context->getSettingsRef().max_threads);
    // pipeline->addTransform(std::make_shared<IntersectOrExceptTransform>(
    //     getOutputStream().header,
    //     distinct ? ASTSelectIntersectExceptQuery::Operator::INTERSECT_DISTINCT : ASTSelectIntersectExceptQuery::Operator::INTERSECT_ALL));

    // processors = collector.detachProcessors();
    // return pipeline;
}

std::shared_ptr<IntersectStepExt> IntersectStepExt::fromProto(const Protos::IntersectStepExt & proto, ContextPtr)
{
    auto [base_input_streams, base_output_stream, output_to_inputs]
        = SetOperationStepExt::deserializeFromProtoBase(proto.query_plan_base());
    auto distinct = proto.distinct();
    auto step = std::make_shared<IntersectStepExt>(base_input_streams, base_output_stream, output_to_inputs, distinct);

    return step;
}

void IntersectStepExt::toProto(Protos::IntersectStepExt & proto, bool) const
{
    SetOperationStepExt::serializeToProtoBase(*proto.mutable_query_plan_base());
    proto.set_distinct(distinct);
}

bool IntersectStepExt::isDistinct() const
{
    return distinct;
}

std::shared_ptr<IQueryPlanStep> IntersectStepExt::copy(ContextPtr) const
{
    return std::make_unique<IntersectStepExt>(input_streams, output_stream.value(), distinct);
}

}
