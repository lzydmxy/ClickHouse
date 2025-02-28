#include <Query/Processors/QueryPlan/MarkDistinctStepExt.h>

// #include <DataTypes/DataTypeHelper.h>
#include <IO/Operators.h>
#include <Interpreters/ExpressionActions.h>
// #include <Interpreters/RuntimeFilter/RuntimeFilterConsumer.h>
#include <QueryPipeline/QueryPipeline.h>
// #include <Query/Processors/Transforms/MarkDistinctTransformExt.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{
MarkDistinctStepExt::MarkDistinctStepExt(const DataStream & input_stream_, String marker_symbol_, std::vector<String> distinct_symbols_)
    : ITransformingStep(input_stream_, /***MarkDistinctTransformExt::transformHeader(input_stream_.header, marker_symbol_)***/ input_stream_.header, {}, true), marker_symbol(marker_symbol_), distinct_symbols(std::move(distinct_symbols_))
{
}

void MarkDistinctStepExt::updateInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
    output_stream = input_streams[0];
    // TODO FIXME DataTypeUInt8
    // output_stream->header.insert(ColumnWithTypeAndName{std::make_shared<DataTypeUInt8>(), marker_symbol});
}

void MarkDistinctStepExt::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    // TODO Add Local Exchange
    // pipeline.resize(1);
    // pipeline.addSimpleTransform([&](const Block & header) { return std::make_shared<MarkDistinctTransformExt>(header, marker_symbol, distinct_symbols); });
}

// std::shared_ptr<MarkDistinctStepExt> MarkDistinctStepExt::fromProto(const Protos::MarkDistinctStepExt & proto, ContextPtr)
// {
//     auto [step_description, base_input_stream] = ITransformingStep::deserializeFromProtoBase(proto.query_plan_base());
//     auto marker_symbol = proto.marker_symbol();
//     std::vector<String> distinct_symbols;
//     for (const auto & element : proto.distinct_symbols())
//         distinct_symbols.emplace_back(element);
//     auto step = std::make_shared<MarkDistinctStepExt>(base_input_stream, marker_symbol, distinct_symbols);
//     step->setStepDescription(step_description);
//     return step;
// }

// void MarkDistinctStepExt::toProto(Protos::MarkDistinctStepExt & proto, bool) const
// {
//     ITransformingStep::serializeToProtoBase(*proto.mutable_query_plan_base());
//     proto.set_marker_symbol(marker_symbol);
//     for (const auto & element : distinct_symbols)
//         proto.add_distinct_symbols(element);
// }

std::shared_ptr<IQueryPlanStep> MarkDistinctStepExt::copy(ContextPtr) const
{
    return std::make_shared<MarkDistinctStepExt>(input_streams[0], marker_symbol, distinct_symbols);
}

}
