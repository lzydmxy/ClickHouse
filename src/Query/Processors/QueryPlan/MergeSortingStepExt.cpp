#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <Query/Processors/QueryPlan/MergeSortingStepExt.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>
#include <Query/Processors/QueryPlan/BuildQueryPipelineSettingsExt.h>


namespace CurrentMetrics
{
extern const Metric TemporaryFilesForSort;
}

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

MergeSortingStepExt::MergeSortingStepExt(
    const DataStream & input_stream_,
    const SortDescription & description_,
    size_t max_merged_block_size_,
    UInt64 limit_,
    size_t max_bytes_before_remerge_,
    double remerge_lowered_memory_bytes_ratio_,
    size_t max_bytes_before_external_sort_,
    TemporaryDataOnDiskScopePtr tmp_data_,
    size_t min_free_disk_space_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits(limit_))
    , description(description_)
    , max_merged_block_size(max_merged_block_size_)
    , limit(limit_)
    , max_bytes_before_remerge(max_bytes_before_remerge_)
    , remerge_lowered_memory_bytes_ratio(remerge_lowered_memory_bytes_ratio_)
    , max_bytes_before_external_sort(max_bytes_before_external_sort_), tmp_data(tmp_data_)
    , min_free_disk_space(min_free_disk_space_)
{
    /// todo: byconity check input_stream is partially sorted by the same description.
    output_stream->sort_description = description;
    output_stream->sort_scope = input_stream_.has_single_port ? DataStream::SortScope::Global
                                                            : DataStream::SortScope::Stream;
}

void MergeSortingStepExt::updateOutputStream()
{
    output_stream = createOutputStream(input_streams.front(), input_streams.front().header, getDataStreamTraits());
    output_stream->sort_description = description;

    output_stream->sort_scope = DataStream::SortScope::Stream;
}

void MergeSortingStepExt::updateLimit(size_t limit_)
{
    if (limit_ && (limit == 0 || limit_ < limit))
    {
        limit = limit_;
        transform_traits.preserves_number_of_rows = false;
    }
}

void MergeSortingStepExt::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    const auto & settings_ext = BuildQueryPipelineSettingsExt::cast(settings);
    max_merged_block_size = settings_ext.context->getSettingsRef().max_block_size;
    max_bytes_before_remerge = settings_ext.context->getSettingsRef().max_bytes_before_remerge_sort;
    remerge_lowered_memory_bytes_ratio = settings_ext.context->getSettingsRef().remerge_sort_lowered_memory_bytes_ratio;
    max_bytes_before_external_sort = settings_ext.context->getSettingsRef().max_bytes_before_external_sort;
    tmp_data = settings_ext.context->getTempDataOnDisk();
    min_free_disk_space = settings_ext.context->getSettingsRef().min_free_disk_space_for_temporary_data;

    bool increase_sort_description_compile_attempts = true;
    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type == QueryPipelineBuilder::StreamType::Totals)
            return nullptr;

        // For multiple FinishSortingTransform we need to count identical comparators only once per QueryPlan.
        // To property support min_count_to_compile_sort_description.
        bool increase_sort_description_compile_attempts_current = increase_sort_description_compile_attempts;

        if (increase_sort_description_compile_attempts)
            increase_sort_description_compile_attempts = false;

        auto tmp_data_on_disk = tmp_data
                ? std::make_unique<TemporaryDataOnDisk>(tmp_data, CurrentMetrics::TemporaryFilesForSort)
                : std::unique_ptr<TemporaryDataOnDisk>();

        return std::make_shared<MergeSortingTransform>(
                header,
                description,
                max_merged_block_size,
                limit,
                increase_sort_description_compile_attempts_current,
                max_bytes_before_remerge / pipeline.getNumStreams(),
                remerge_lowered_memory_bytes_ratio,
                max_bytes_before_external_sort,
                std::move(tmp_data_on_disk),
                min_free_disk_space);
    });
}

void MergeSortingStepExt::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << "Sort description: ";
    dumpSortDescription(description, settings.out);
    settings.out << '\n';

    if (limit)
        settings.out << prefix << "Limit " << limit << '\n';
}

void MergeSortingStepExt::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Sort Description", explainSortDescription(description));

    if (limit)
        map.add("Limit", limit);
}

std::shared_ptr<IQueryPlanStep> MergeSortingStepExt::copy(ContextPtr) const
{
    return std::make_shared<MergeSortingStepExt>(
        input_streams[0],
        description,
        max_merged_block_size,
        limit,
        max_bytes_before_remerge,
        remerge_lowered_memory_bytes_ratio,
        max_bytes_before_external_sort,
        tmp_data,
        min_free_disk_space);
}


void MergeSortingStepExt::toProto(Protos::MergeSortingStepExt & proto, bool) const
{
    ProtosSerDerHelper::serializeToProtoBase(*this, *proto.mutable_query_plan_base());
    for (const auto & element : description)
        ProtosSerDerHelper::toProto(element, *proto.add_description());
    proto.set_max_merged_block_size(max_merged_block_size);
    proto.set_limit(limit);
    proto.set_max_bytes_before_remerge(max_bytes_before_remerge);
    proto.set_remerge_lowered_memory_bytes_ratio(remerge_lowered_memory_bytes_ratio);
    proto.set_max_bytes_before_external_sort(max_bytes_before_external_sort);
    proto.set_min_free_disk_space(min_free_disk_space);
}

std::shared_ptr<MergeSortingStepExt> MergeSortingStepExt::fromProto(const Protos::MergeSortingStepExt & proto, ContextPtr context)
{
    auto [step_description, base_input_stream] = ProtosSerDerHelper::deserializeFromProtoBase(proto.query_plan_base());
    SortDescription description;
    for (const auto & proto_element : proto.description())
    {
        SortColumnDescription element;
        ProtosSerDerHelper::fillFromProto(element, proto_element);
        description.emplace_back(std::move(element));
    }
    auto max_merged_block_size = proto.max_merged_block_size();
    auto limit = proto.limit();
    auto max_bytes_before_remerge = proto.max_bytes_before_remerge();
    auto remerge_lowered_memory_bytes_ratio = proto.remerge_lowered_memory_bytes_ratio();
    auto max_bytes_before_external_sort = proto.max_bytes_before_external_sort();
    auto tmp_data = context ? context->getTempDataOnDisk() : nullptr;
    auto min_free_disk_space = proto.min_free_disk_space();
    auto step = std::make_shared<MergeSortingStepExt>(
        base_input_stream,
        description,
        max_merged_block_size,
        limit,
        max_bytes_before_remerge,
        remerge_lowered_memory_bytes_ratio,
        max_bytes_before_external_sort,
        tmp_data,
        min_free_disk_space);
    step->setStepDescription(step_description);
    return step;
}
}
