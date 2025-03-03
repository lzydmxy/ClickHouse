#include <DataTypes/DataTypeString.h>
#include <Query/Processors/QueryPlan/ExplainAnalyzeStepExt.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

ExplainAnalyzeStepExt::ExplainAnalyzeStepExt(
    const DataStream & input_stream_,
    const String & output_name_,
    ASTExplainQueryExt::ExplainKindExt kind_,
    ContextMutablePtr context_,
    std::shared_ptr<QueryPlan> query_plan_ptr_,
    QueryPlanSettings settings_)
    : ITransformingStep(input_stream_, {{std::make_shared<DataTypeString>(), output_name_}}, {})
    , kind(kind_)
    , context(context_)
    , query_plan_ptr(query_plan_ptr_)
    , settings(settings_)
{
}

void ExplainAnalyzeStepExt::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    // TODO: implement
    // if (!query_plan_ptr)
    //     throw Exception(ErrorCodes::LOGICAL_ERROR, "QueryPlan is not set");
    // pipeline.resize(1);
    // pipeline.addSimpleTransform(
    //     [&](const Block & header)
    //     {
    //         return std::make_shared<ExplainAnalyzeTransform>(
    //             header, output_stream->header, kind, query_plan_ptr, context, segment_descriptions, settings);
    //     });
}

std::shared_ptr<IQueryPlanStep> ExplainAnalyzeStepExt::copy(ContextPtr) const
{
    return std::make_shared<ExplainAnalyzeStepExt>(input_streams[0], getOutputName(), kind, context, query_plan_ptr, settings);
}

}
