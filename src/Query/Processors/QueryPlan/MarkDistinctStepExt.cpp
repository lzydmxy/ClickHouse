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

}
