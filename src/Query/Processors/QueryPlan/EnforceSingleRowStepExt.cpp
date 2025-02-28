#include <Query/Common/join_common.h>
#include <Query/Processors/QueryPlan/EnforceSingleRowStepExt.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{
EnforceSingleRowStepExt::EnforceSingleRowStepExt(const DB::DataStream & input_stream_)
    : ITransformingStep(input_stream_, input_stream_.header, {})
{
    makeOutputNullable();
}

void EnforceSingleRowStepExt::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    // TODO: implement
    // pipeline.resize(1);
    // pipeline.addSimpleTransform([&](const Block & header) { return std::make_shared<EnforceSingleRowTransform>(header); });
}

void EnforceSingleRowStepExt::updateOutputStream()
{
    makeOutputNullable();
}

void EnforceSingleRowStepExt::makeOutputNullable()
{
    auto input_header = input_streams[0].header;
    NamesAndTypes nullable_output_header;
    for (auto & input : input_header)
        if (!JoinCommon::canBecomeNullable(input.type))
            nullable_output_header.emplace_back(input.name, input.type);
        else
            nullable_output_header.emplace_back(input.name, JoinCommon::convertTypeToNullable(input.type));
    // FIXME: No matching constructor for initialization of 'Block'
    // output_stream = DataStream{.header = {nullable_output_header}};
}

}
