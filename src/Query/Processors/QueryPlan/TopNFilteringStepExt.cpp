#include <Query/Processors/QueryPlan/TopNFilteringStepExt.h>

#include <IO/Operators.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Processors/Transforms/PartialSortingTransform.h>
// #include <Query/Processors/Transforms/TopNFilteringTransformExt.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INVALID_SETTING_VALUE;
}

TopNFilteringStepExt::TopNFilteringStepExt(
    const DataStream & input_stream_, SortDescription sort_description_, UInt64 size_, TopNModel model_, TopNFilteringAlgorithm algorithm_)
    : ITransformingStep(input_stream_, input_stream_.header, {})
    , sort_description(std::move(sort_description_))
    , size(size_)
    , model(model_)
    , algorithm(algorithm_)
{}

void TopNFilteringStepExt::updateInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
    output_stream->header = input_streams_[0].header;
}

void TopNFilteringStepExt::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    // if (algorithm == TopNFilteringAlgorithm::Unspecified)
    // {
    //     // String default_algorithm = settings.context->getSettingsRef().topn_filtering_algorithm_for_unsorted_stream;
    //     //TODO read from settings
    //     String default_algorithm = "SortAndLimit";

    //     if (default_algorithm == "SortAndLimit")
    //         algorithm = TopNFilteringAlgorithm::SortAndLimit;
    //     else if (default_algorithm == "Heap")
    //         algorithm = TopNFilteringAlgorithm::Heap;
    //     else
    //         throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Invalid setting value for topn_filtering_algorithm_for_unsorted_stream");
    // }

    // switch (algorithm)
    // {
    //     case TopNFilteringAlgorithm::SortAndLimit:
    //         pipeline.addSimpleTransform(
    //             [&](const Block & header) { return std::make_shared<PartialSortingTransform>(header, sort_description, 0); });
    //         pipeline.addSimpleTransform([&](const Block & header) {
    //             return std::make_shared<TopNFilteringByLimitingTransformExt>(header, sort_description, size, model);
    //         });
    //         break;
    //     case TopNFilteringAlgorithm::Limit:
    //         pipeline.addSimpleTransform([&](const Block & header) {
    //             return std::make_shared<TopNFilteringByLimitingTransformExt>(header, sort_description, size, model);
    //         });
    //         break;
    //     case TopNFilteringAlgorithm::Heap:
    //         pipeline.addSimpleTransform([&](const Block & header) {
    //             return std::make_shared<TopNFilteringByHeapTransformExt>(header, sort_description, size, model);
    //         });
    //         break;
    //     default:
    //         throw Exception(
    //             ErrorCodes::NOT_IMPLEMENTED,
    //             "Not implemented topn filtering algorithm `{}` is used",
    //             TopNFilteringAlgorithmConverter::toString(algorithm));
    // }
}

void TopNFilteringStepExt::describeActions(FormatSettings &) const
{
}

void TopNFilteringStepExt::describeActions(JSONBuilder::JSONMap &) const
{
}
}
