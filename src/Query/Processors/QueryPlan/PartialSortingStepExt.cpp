#include <Query/Processors/QueryPlan/PartialSortingStepExt.h>
#include <Query/Processors/QueryPlan/BuildQueryPipelineSettingsExt.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <Processors/Transforms/LimitsCheckingTransform.h>
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
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = limit == 0,
        }
    };
}

PartialSortingStepExt::PartialSortingStepExt(
    const DataStream & input_stream_,
    SortDescription sort_description_,
    UInt64 limit_,
    SizeLimits size_limits_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits(limit_))
    , sort_description(std::move(sort_description_))
    , limit(limit_)
    , size_limits(size_limits_)
{
    output_stream->sort_description = sort_description;
    output_stream->sort_scope = DataStream::SortScope::Chunk;
}

void PartialSortingStepExt::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
    output_stream->header = input_streams_[0].header;
}

void PartialSortingStepExt::updateLimit(size_t limit_)
{
    if (limit_ && (limit == 0 || limit_ < limit))
    {
        limit = limit_;
        transform_traits.preserves_number_of_rows = false;
    }
}

void PartialSortingStepExt::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    const auto & settings_ext = BuildQueryPipelineSettingsExt::cast(settings);

    if (size_limits.max_rows == 0)
    {
        size_limits.max_rows = settings_ext.context->getSettingsRef().max_rows_to_sort;
        size_limits.max_bytes= settings_ext.context->getSettingsRef().max_bytes_to_sort;
        size_limits.overflow_mode = settings_ext.context->getSettingsRef().sort_overflow_mode;
    }

    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type != QueryPipelineBuilder::StreamType::Main)
            return nullptr;

        return std::make_shared<PartialSortingTransform>(header, sort_description, limit);
    });

    StreamLocalLimits limits;
    limits.mode = LimitsMode::LIMITS_CURRENT; //-V1048
    limits.size_limits = size_limits;

    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type != QueryPipelineBuilder::StreamType::Main)
            return nullptr;

        auto transform = std::make_shared<LimitsCheckingTransform>(header, limits);
        return transform;
    });
}

void PartialSortingStepExt::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << "Sort description: ";
    dumpSortDescription(sort_description, settings.out);
    settings.out << '\n';

    if (limit)
        settings.out << prefix << "Limit " << limit << '\n';
}

void PartialSortingStepExt::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Sort Description", explainSortDescription(sort_description));

    if (limit)
        map.add("Limit", limit);
}

std::shared_ptr<PartialSortingStepExt> PartialSortingStepExt::fromProto(const Protos::PartialSortingStepExt & proto, ContextPtr)
{
    auto [step_description, base_input_stream] = ProtosSerDerHelper::deserializeFromProtoBase(proto.query_plan_base());
    SortDescription sort_description;
    for (const auto & proto_element : proto.sort_description())
    {
        SortColumnDescription element;
        ProtosSerDerHelper::fillFromProto(element, proto_element);
        sort_description.emplace_back(std::move(element));
    }
    auto limit = proto.limit();
    SizeLimits size_limits;
    ProtosSerDerHelper::fillFromProto(size_limits, proto.size_limits());
    auto step = std::make_shared<PartialSortingStepExt>(base_input_stream, sort_description, limit, size_limits);
    step->setStepDescription(step_description);
    return step;
}

void PartialSortingStepExt::toProto(Protos::PartialSortingStepExt & proto, bool) const
{
    ProtosSerDerHelper::serializeToProtoBase(*this, *proto.mutable_query_plan_base());
    for (const auto & element : sort_description)
        ProtosSerDerHelper::toProto(element, *proto.add_sort_description());
    proto.set_limit(limit);
    ProtosSerDerHelper::toProto(size_limits, *proto.mutable_size_limits());
}

std::shared_ptr<IQueryPlanStep> PartialSortingStepExt::copy(ContextPtr) const
{
    return std::make_shared<PartialSortingStepExt>(input_streams[0], sort_description, limit, size_limits);
}

}
