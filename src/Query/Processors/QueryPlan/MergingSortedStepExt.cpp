#include <IO/Operators.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Query/Processors/QueryPlan/MergingSortedStepExt.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
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

MergingSortedStepExt::MergingSortedStepExt(
    const DataStream & input_stream_,
    SortDescription sort_description_,
    size_t max_block_size_,
    UInt64 limit_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits(limit_))
    , sort_description(std::move(sort_description_))
    , max_block_size(max_block_size_)
    , limit(limit_)
{
    /// todo: byconity check input_stream is partially sorted (each port) by the same description.
    output_stream->sort_description = sort_description;
    output_stream->sort_scope = DataStream::SortScope::Global;
}

void MergingSortedStepExt::updateOutputStream()
{
    output_stream = createOutputStream(input_streams.front(), input_streams.front().header, getDataStreamTraits());
    output_stream->sort_description = sort_description;

    output_stream->sort_scope = DataStream::SortScope::Global;
}

void MergingSortedStepExt::updateLimit(size_t limit_)
{
    if (limit_ && (limit == 0 || limit_ < limit))
    {
        limit = limit_;
        transform_traits.preserves_number_of_rows = false;
    }
}

void MergingSortedStepExt::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    /// If there are several streams, then we merge them into one
    if (pipeline.getNumStreams() > 1)
    {

        auto transform = std::make_shared<MergingSortedTransform>(
                pipeline.getHeader(),
                pipeline.getNumStreams(),
                sort_description,
                max_block_size, 0, SortingQueueStrategy::Batch, limit);

        pipeline.addTransform(std::move(transform));
    }
}

void MergingSortedStepExt::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << "Sort description: ";
    dumpSortDescription(sort_description, settings.out);
    settings.out << '\n';

    if (limit)
        settings.out << prefix << "Limit " << limit << '\n';
}

void MergingSortedStepExt::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Sort Description", explainSortDescription(sort_description));

    if (limit)
        map.add("Limit", limit);
}

std::shared_ptr<MergingSortedStepExt> MergingSortedStepExt::fromProto(const Protos::MergingSortedStepExt & proto, ContextPtr)
{
    auto [step_description, base_input_stream] = ProtosSerDerHelper::deserializeFromProtoBase(proto.query_plan_base());
    SortDescription sort_description;
    for (const auto & proto_element : proto.sort_description())
    {
        SortColumnDescription element;
        ProtosSerDerHelper::fillFromProto(element, proto_element);
        sort_description.emplace_back(std::move(element));
    }
    auto max_block_size = proto.max_block_size();
    auto limit = proto.limit();
    auto step = std::make_shared<MergingSortedStepExt>(base_input_stream, sort_description, max_block_size, limit);
    step->setStepDescription(step_description);
    return step;
}

void MergingSortedStepExt::toProto(Protos::MergingSortedStepExt & proto, bool) const
{
    ProtosSerDerHelper::serializeToProtoBase(*this, *proto.mutable_query_plan_base());
    for (const auto & element : sort_description)
        ProtosSerDerHelper::toProto(element, *proto.add_sort_description());
    proto.set_max_block_size(max_block_size);
    proto.set_limit(limit);
}

std::shared_ptr<IQueryPlanStep> MergingSortedStepExt::copy(ContextPtr) const
{
    return std::make_shared<MergingSortedStepExt>(input_streams[0], sort_description, max_block_size, limit);
}

}
