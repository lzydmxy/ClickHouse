#include <Query/Processors/QueryPlan/FinishSortingStepExt.h>

#include <Processors/Transforms/DistinctTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <Processors/Transforms/FinishSortingTransform.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>

namespace DB
{

static ITransformingStep::Traits getTraits(size_t limit)
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = limit == 0,
        }
    };
}

FinishSortingStepExt::FinishSortingStepExt(
    const DataStream & input_stream_,
    SortDescription prefix_description_,
    SortDescription result_description_,
    size_t max_block_size_,
    UInt64 limit_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits(limit_))
    , prefix_description(std::move(prefix_description_))
    , result_description(std::move(result_description_))
    , max_block_size(max_block_size_)
    , limit(limit_)
{
    /// todo: bc, check input_stream is sorted by prefix_description.
    output_stream->sort_description = result_description;
    output_stream->sort_scope= DataStream::SortScope::Global;
}

void FinishSortingStepExt::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
    output_stream->header = input_streams_[0].header;
}

void FinishSortingStepExt::updateLimit(size_t limit_)
{
    if (limit_ && (limit == 0 || limit_ < limit))
    {
        limit = limit_;
        transform_traits.preserves_number_of_rows = false;
    }
}

void FinishSortingStepExt::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    bool need_finish_sorting = (prefix_description.size() < result_description.size());
    if (pipeline.getNumStreams() > 1)
    {
        UInt64 limit_for_merging = (need_finish_sorting ? 0 : limit);
        auto transform = std::make_shared<MergingSortedTransform>(
                pipeline.getHeader(),
                pipeline.getNumStreams(),
                prefix_description,
                max_block_size,
                0,
                SortingQueueStrategy::Batch,
                limit_for_merging);

        pipeline.addTransform(std::move(transform));
    }

    if (need_finish_sorting)
    {
        pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type != QueryPipelineBuilder::StreamType::Main)
                return nullptr;

            return std::make_shared<PartialSortingTransform>(header, result_description, limit);
        });
        bool increase_sort_description_compile_attempts = true;

        /// NOTE limits are not applied to the size of temporary sets in FinishSortingTransform
        pipeline.addSimpleTransform([&, increase_sort_description_compile_attempts](const Block & header) mutable -> ProcessorPtr
        {
            bool increase_sort_description_compile_attempts_current = increase_sort_description_compile_attempts;

            if (increase_sort_description_compile_attempts)
                increase_sort_description_compile_attempts = false;
            return std::make_shared<FinishSortingTransform>(
                header, prefix_description, result_description, max_block_size, limit, increase_sort_description_compile_attempts_current);
        });
    }
}

void FinishSortingStepExt::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');

    settings.out << prefix << "Prefix sort description: ";
    dumpSortDescription(prefix_description, settings.out);
    settings.out << '\n';

    settings.out << prefix << "Result sort description: ";
    dumpSortDescription(result_description, settings.out);
    settings.out << '\n';

    if (limit)
        settings.out << prefix << "Limit " << limit << '\n';
}

void FinishSortingStepExt::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Prefix Sort Description", explainSortDescription(prefix_description));
    map.add("Result Sort Description", explainSortDescription(result_description));

    if (limit)
        map.add("Limit", limit);
}

std::shared_ptr<FinishSortingStepExt> FinishSortingStepExt::fromProto(const Protos::FinishSortingStepExt & proto, ContextPtr)
{
    auto [step_description, base_input_stream] = ProtosSerDerHelper::deserializeFromProtoBase(proto.query_plan_base());
    SortDescription prefix_description;
    for (const auto & proto_element : proto.prefix_description())
    {
        SortColumnDescription element;
        ProtosSerDerHelper::fillFromProto(element, proto_element);
        prefix_description.emplace_back(std::move(element));
    }
    SortDescription result_description;
    for (const auto & proto_element : proto.result_description())
    {
        SortColumnDescription element;
        ProtosSerDerHelper::fillFromProto(element, proto_element);
        result_description.emplace_back(std::move(element));
    }
    auto max_block_size = proto.max_block_size();
    auto limit = proto.limit();
    auto step = std::make_shared<FinishSortingStepExt>(base_input_stream, prefix_description, result_description, max_block_size, limit);
    step->setStepDescription(step_description);
    return step;
}

void FinishSortingStepExt::toProto(Protos::FinishSortingStepExt & proto, bool) const
{
    ProtosSerDerHelper::serializeToProtoBase(*this, *proto.mutable_query_plan_base());
    for (const auto & element : prefix_description)
        ProtosSerDerHelper::toProto(element, *proto.add_prefix_description());
    for (const auto & element : result_description)
        ProtosSerDerHelper::toProto(element, *proto.add_result_description());
    proto.set_max_block_size(max_block_size);
    proto.set_limit(limit);
}

std::shared_ptr<IQueryPlanStep> FinishSortingStepExt::copy(ContextPtr) const
{
    return std::make_shared<FinishSortingStepExt>(input_streams[0], prefix_description, result_description, max_block_size, limit);
}

}
