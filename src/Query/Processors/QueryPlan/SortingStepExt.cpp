#include <Query/Processors/QueryPlan/SortingStepExt.h>

#include <Core/SettingsEnums.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Processors/LimitTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Transforms/FinishSortingTransform.h>
#include <Processors/Transforms/LimitsCheckingTransform.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <Common/JSONBuilder.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>
#include <Query/Common/OptimizerContext.h>
#include <Query/Processors/QueryPlan/BuildQueryPipelineSettingsExt.h>

namespace CurrentMetrics
{
    extern const Metric TemporaryFilesForSort;
}

namespace DB
{

static ITransformingStep::Traits getTraits(const size_t & limit, bool is_final_sorting = false)
{
    return ITransformingStep::Traits{
        {
            .returns_single_stream = is_final_sorting,
            .preserves_number_of_streams = !is_final_sorting,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = limit == 0,
        }};
}

SortingStepExt::SortingStepExt(
    const DataStream & input_stream_,
    SortDescription result_description_,
    size_t limit_,
    Stage stage_,
    SortDescription prefix_description_,
    bool enable_adaptive_spill_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits(limit_, stage_ != Stage::PARTIAL && stage_ != Stage::PARTIAL_NO_MERGE))
    , result_description(result_description_)
    , limit(limit_)
    , stage(stage_)
    , prefix_description(prefix_description_)
    , enable_adaptive_spill(enable_adaptive_spill_)
{
    /// todo: bc, check input_stream is partially sorted by the same description.
    /// todo: bc, support mannual/auto spill
    output_stream->sort_description = result_description;
    output_stream->sort_scope
        = (input_stream_.has_single_port || (stage_ != Stage::PARTIAL && stage_ != Stage::PARTIAL_NO_MERGE)) ? DataStream::SortScope::Global : DataStream::SortScope::Stream;
}

void SortingStepExt::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
    output_stream->header = input_streams_[0].header;
}

void SortingStepExt::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    const auto & settings_ext = BuildQueryPipelineSettingsExt::cast(settings);
    auto local_settings = settings_ext.context->getSettingsRef();
    SizeLimits size_limits(local_settings.max_rows_to_sort, local_settings.max_bytes_to_sort, local_settings.sort_overflow_mode);

    auto desc_copy = result_description;

    if (stage == Stage::FULL || stage == Stage::PARTIAL || stage == Stage::PARTIAL_NO_MERGE)
    {
        // finish sorting
        if (!prefix_description.empty())
        {
            bool need_finish_sorting = (prefix_description.size() < result_description.size());

            if (!need_finish_sorting)
            {
                if (pipeline.getNumStreams() > 1 && stage != Stage::PARTIAL_NO_MERGE)
                {
                    auto transform = std::make_shared<MergingSortedTransform>(
                        pipeline.getHeader(),
                        pipeline.getNumStreams(),
                        prefix_description,
                        local_settings.max_block_size,
                        0,
                        SortingQueueStrategy::Batch,
                        limit);

                    pipeline.addTransform(std::move(transform));
                }
                if (limit > 0)
                {
                    auto transform = std::make_shared<LimitTransform>(
                        pipeline.getHeader(), limit, 0, pipeline.getNumStreams(), false, false, result_description);
                    pipeline.addTransform(std::move(transform));
                }
                return;
            }

            if (pipeline.getNumStreams() > 1)
            {
                UInt64 limit_for_merging = 0; // need_finish_sorting
                auto transform = std::make_shared<MergingSortedTransform>(
                    pipeline.getHeader(),
                    pipeline.getNumStreams(),
                    prefix_description,
                    local_settings.max_block_size,
                    0,
                    SortingQueueStrategy::Batch,
                    limit_for_merging);

                pipeline.addTransform(std::move(transform));
            }

            pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr {
                if (stream_type != QueryPipelineBuilder::StreamType::Main)
                    return nullptr;

                return std::make_shared<PartialSortingTransform>(header, result_description, limit);
            });

            bool increase_sort_description_compile_attempts = true;
            /// NOTE limits are not applied to the size of temporary sets in FinishSortingTransform
            pipeline.addSimpleTransform([&, increase_sort_description_compile_attempts](const Block & header) mutable -> ProcessorPtr {
                bool increase_sort_description_compile_attempts_current = increase_sort_description_compile_attempts;

                if (increase_sort_description_compile_attempts)
                    increase_sort_description_compile_attempts = false;

                return std::make_shared<FinishSortingTransform>(
                    header, prefix_description, result_description, local_settings.max_block_size, limit, increase_sort_description_compile_attempts_current);
            });
            return;
        }

        pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr {
            if (stream_type != QueryPipelineBuilder::StreamType::Main)
                return nullptr;

            return std::make_shared<PartialSortingTransform>(header, desc_copy, limit);
        });

        StreamLocalLimits limits;
        limits.mode = LimitsMode::LIMITS_CURRENT; //-V1048
        limits.size_limits = size_limits;

        pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr {
            if (stream_type != QueryPipelineBuilder::StreamType::Main)
                return nullptr;

            auto transform = std::make_shared<LimitsCheckingTransform>(header, limits);
            return transform;
        });

        bool increase_sort_description_compile_attempts = true;

        pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr {
            if (stream_type == QueryPipelineBuilder::StreamType::Totals)
                return nullptr;
            bool increase_sort_description_compile_attempts_current = increase_sort_description_compile_attempts;

            if (increase_sort_description_compile_attempts)
                increase_sort_description_compile_attempts = false;

            auto tmp_data = settings_ext.context->getTempDataOnDisk();

            auto tmp_data_on_disk = tmp_data
                ? std::make_unique<TemporaryDataOnDisk>(tmp_data, CurrentMetrics::TemporaryFilesForSort)
                : std::unique_ptr<TemporaryDataOnDisk>();

            return std::make_shared<MergeSortingTransform>(
                header,
                result_description,
                local_settings.max_block_size,
                limit,
                increase_sort_description_compile_attempts_current,
                local_settings.max_bytes_before_remerge_sort / pipeline.getNumStreams(),
                local_settings.remerge_sort_lowered_memory_bytes_ratio,
                local_settings.max_bytes_before_external_sort,
                std::move(tmp_data_on_disk),
                local_settings.min_free_disk_space_for_temporary_data);
        });

        /// If there are several streams, then we merge them into one
        if (pipeline.getNumStreams() > 1 && stage != Stage::PARTIAL_NO_MERGE)
        {
            auto transform = std::make_shared<MergingSortedTransform>(
                pipeline.getHeader(),
                pipeline.getNumStreams(),
                desc_copy,
                local_settings.max_block_size,
                0,
                SortingQueueStrategy::Batch,
                limit);

            pipeline.addTransform(std::move(transform));
        }
        return;
    }

    /// If there are several streams, then we merge them into one
    if (pipeline.getNumStreams() > 1)
    {
        auto transform = std::make_shared<MergingSortedTransform>(
            pipeline.getHeader(),
            pipeline.getNumStreams(),
            desc_copy,
            local_settings.max_block_size,
            0,
            SortingQueueStrategy::Batch,
            limit);

        pipeline.addTransform(std::move(transform));
    }
}

template <class... Ts>
struct overloaded : Ts...
{
    using Ts::operator()...;
};

void SortingStepExt::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << "Sort description: ";
    dumpSortDescription(result_description, settings.out);
    settings.out << '\n';
    settings.out << prefix << "Limit " << limit << '\n';
}

void SortingStepExt::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Sort Description", explainSortDescription(result_description));
    map.add("Limit", limit);
}

std::shared_ptr<SortingStepExt> SortingStepExt::fromProto(const Protos::SortingStepExt & proto, ContextPtr)
{
    auto [step_description, base_input_stream] = ProtosSerDerHelper::deserializeFromProtoBase(proto.query_plan_base());
    SortDescription result_description;
    for (const auto & proto_element : proto.result_description())
    {
        SortColumnDescription element;
        ProtosSerDerHelper::fillFromProto(element, proto_element);
        result_description.emplace_back(std::move(element));
    }

    Stage stage = Stage::FULL;
    if (proto.has_stage())
        stage = StageConverter::fromProto(proto.stage());

    auto limit = proto.limit();
    SortDescription prefix_description;
    for (const auto & proto_element : proto.prefix_description())
    {
        SortColumnDescription element;
        ProtosSerDerHelper::fillFromProto(element, proto_element);
        prefix_description.emplace_back(std::move(element));
    }
    auto step = std::make_shared<SortingStepExt>(base_input_stream, result_description, limit, stage, prefix_description);
    step->setStepDescription(step_description);
    return step;
}

void SortingStepExt::toProto(Protos::SortingStepExt & proto, bool) const
{
    ProtosSerDerHelper::serializeToProtoBase(*this, *proto.mutable_query_plan_base());
    for (const auto & element : result_description)
        ProtosSerDerHelper::toProto(element, *proto.add_result_description());
    proto.set_limit(limit);
    proto.set_partial(false);
    proto.set_stage(StageConverter::toProto(stage));
    for (const auto & element : prefix_description)
        ProtosSerDerHelper::toProto(element, *proto.add_prefix_description());
}

std::shared_ptr<IQueryPlanStep> SortingStepExt::copy(ContextPtr) const
{
    return std::make_shared<SortingStepExt>(input_streams[0], result_description, limit, stage, prefix_description, enable_adaptive_spill);
}

void SortingStepExt::updateOutputStream()
{
    output_stream = createOutputStream(input_streams.front(), input_streams.front().header, getDataStreamTraits());
    output_stream->sort_description = result_description;
    output_stream->sort_scope = DataStream::SortScope::Global;
}
}
