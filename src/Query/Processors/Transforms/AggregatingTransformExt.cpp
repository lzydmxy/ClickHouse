#include <Processors/ISource.h>
#include <Query/Processors/Transforms/AggregatingTransformExt.h>
#include <Query/Processors/Transforms/MergingAggregatedMemoryEfficientTransformExt.h>
#include <Query/Protos/plan_node.pb.h>
#include <QueryPipeline/Pipe.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
extern const Event ExternalAggregationMerge;
}

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
extern const int LOGICAL_ERROR;
}

namespace
{

const AggregatedChunkInfo * getInfoFromChunk(const Chunk & chunk)
{
    const auto & info = chunk.getChunkInfo();
    if (!info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk info was not set for chunk.");

    const auto * agg_info = typeid_cast<const AggregatedChunkInfo *>(info.get());
    if (!agg_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk should have AggregatedChunkInfo.");

    return agg_info;
}

/// Reads chunks from file in native format. Provide chunks with aggregation info.
class SourceFromNativeStreamExt : public ISource
{
public:
    explicit SourceFromNativeStreamExt(TemporaryFileStream * tmp_stream_) : ISource(tmp_stream_->getHeader()), tmp_stream(tmp_stream_) { }

    String getName() const override { return "SourceFromNativeStreamExt"; }

    Chunk generate() override
    {
        if (!tmp_stream)
            return {};

        auto block = tmp_stream->read();
        if (!block)
        {
            tmp_stream = nullptr;
            return {};
        }
        return convertToChunk(block);
    }

    std::optional<ReadProgress> getReadProgress() override { return std::nullopt; }

private:
    TemporaryFileStream * tmp_stream;
};

}

/// Worker which merges buckets for two-level aggregation.
/// Atomically increments bucket counter and returns merged result.
class ConvertingAggregatedToChunksSourceExt : public ISource
{
public:
    static constexpr UInt32 NUM_BUCKETS = 256;

    struct SharedData
    {
        std::atomic<UInt32> next_bucket_to_merge = 0;
        std::array<std::atomic<bool>, NUM_BUCKETS> is_bucket_processed{};
        std::atomic<bool> is_cancelled = false;

        SharedData()
        {
            for (auto & flag : is_bucket_processed)
                flag = false;
        }
    };

    using SharedDataPtr = std::shared_ptr<SharedData>;

    ConvertingAggregatedToChunksSourceExt(
        AggregatingTransformParamsExtPtr params_, ManyAggregatedDataVariantsPtr data_, SharedDataPtr shared_data_, Arena * arena_)
        : ISource(params_->getHeader())
        , params(std::move(params_))
        , data(std::move(data_))
        , shared_data(std::move(shared_data_))
        , arena(arena_)
    {
    }

    String getName() const override { return "ConvertingAggregatedToChunksSourceExt"; }

protected:
    Chunk generate() override
    {
        UInt32 bucket_num = shared_data->next_bucket_to_merge.fetch_add(1);

        if (bucket_num >= NUM_BUCKETS)
            return {};

        Block block
            = params->aggregator_ext.mergeAndConvertOneBucketToBlock(*data, arena, params->final, bucket_num, &shared_data->is_cancelled);
        Chunk chunk = convertToChunk(block);

        shared_data->is_bucket_processed[bucket_num] = true;

        return chunk;
    }

private:
    AggregatingTransformParamsExtPtr params;
    ManyAggregatedDataVariantsPtr data;
    SharedDataPtr shared_data;
    Arena * arena;
};

/// Generates chunks with aggregated data.
/// In single level case, aggregates data itself.
/// In two-level case, creates `ConvertingAggregatedToChunksSource` workers:
///
/// ConvertingAggregatedToChunksSource ->
/// ConvertingAggregatedToChunksSource -> ConvertingAggregatedToChunksTransform -> AggregatingTransform
/// ConvertingAggregatedToChunksSource ->
///
/// Result chunks guaranteed to be sorted by bucket number.
class ConvertingAggregatedToChunksTransformExt : public IProcessor
{
public:
    ConvertingAggregatedToChunksTransformExt(
        AggregatingTransformParamsExtPtr params_, ManyAggregatedDataVariantsPtr data_, size_t num_threads_)
        : IProcessor({}, {params_->getHeader()})
        , params(std::move(params_))
        , data(std::move(data_))
        , shared_data(std::make_shared<ConvertingAggregatedToChunksSourceExt::SharedData>())
        , num_threads(num_threads_)
    {
    }

    String getName() const override { return "ConvertingAggregatedToChunksTransformExt"; }

    void work() override
    {
        if (data->empty())
        {
            finished = true;
            return;
        }

        if (!is_initialized)
        {
            initialize();
            return;
        }

        if (data->at(0)->isTwoLevel())
        {
            /// In two-level case will only create sources.
            if (inputs.empty())
                createSources();
        }
        else
        {
            mergeSingleLevel();
        }
    }

    Processors expandPipeline() override
    {
        for (auto & source : processors)
        {
            auto & out = source->getOutputs().front();
            inputs.emplace_back(out.getHeader(), this);
            connect(out, inputs.back());
            inputs.back().setNeeded();
        }

        return std::move(processors);
    }

    IProcessor::Status prepare() override
    {
        auto & output = outputs.front();

        if (finished && !has_input)
        {
            output.finish();
            return Status::Finished;
        }

        /// Check can output.
        if (output.isFinished())
        {
            for (auto & input : inputs)
                input.close();

            shared_data->is_cancelled.store(true);

            return Status::Finished;
        }

        if (!output.canPush())
            return Status::PortFull;

        if (!is_initialized)
            return Status::Ready;

        if (!processors.empty())
            return Status::ExpandPipeline;

        if (has_input)
            return preparePushToOutput();

        /// Single level case.
        if (inputs.empty())
            return Status::Ready;

        /// Two-level case.
        return prepareTwoLevel();
    }

    void onCancel() override { shared_data->is_cancelled.store(true, std::memory_order_seq_cst); }

private:
    IProcessor::Status preparePushToOutput()
    {
        auto & output = outputs.front();
        output.push(std::move(current_chunk));
        has_input = false;

        if (finished)
        {
            output.finish();
            return Status::Finished;
        }

        return Status::PortFull;
    }

    /// Read all sources and try to push current bucket.
    IProcessor::Status prepareTwoLevel()
    {
        auto & output = outputs.front();

        for (auto & input : inputs)
        {
            if (!input.isFinished() && input.hasData())
            {
                auto chunk = input.pull();
                auto bucket = getInfoFromChunk(chunk)->bucket_num;
                chunks[bucket] = std::move(chunk);
            }
        }

        if (!shared_data->is_bucket_processed[current_bucket_num])
            return Status::NeedData;

        if (!chunks[current_bucket_num])
            return Status::NeedData;

        output.push(std::move(chunks[current_bucket_num]));

        ++current_bucket_num;
        if (current_bucket_num == NUM_BUCKETS)
        {
            output.finish();
            /// Do not close inputs, they must be finished.
            return Status::Finished;
        }

        return Status::PortFull;
    }

    AggregatingTransformParamsExtPtr params;
    ManyAggregatedDataVariantsPtr data;
    ConvertingAggregatedToChunksSourceExt::SharedDataPtr shared_data;

    size_t num_threads;

    bool is_initialized = false;
    bool has_input = false;
    bool finished = false;

    Chunk current_chunk;

    UInt32 current_bucket_num = 0;
    static constexpr Int32 NUM_BUCKETS = 256;
    std::array<Chunk, NUM_BUCKETS> chunks;

    Processors processors;

    void setCurrentChunk(Chunk chunk)
    {
        if (has_input)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Current chunk was already set in ConvertingAggregatedToChunksTransformExt.");

        has_input = true;
        current_chunk = std::move(chunk);
    }

    void initialize()
    {
        is_initialized = true;

        AggregatedDataVariantsPtr & first = data->at(0);

        /// At least we need one arena in first data item per thread
        if (num_threads > first->aggregates_pools.size())
        {
            Arenas & first_pool = first->aggregates_pools;
            for (size_t j = first_pool.size(); j < num_threads; j++)
                first_pool.emplace_back(std::make_shared<Arena>());
        }

        if (first->type == AggregatedDataVariants::Type::without_key || params->params.overflow_row)
        {
            params->aggregator_ext.mergeWithoutKeyDataImpl(*data);
            auto block = params->aggregator_ext.prepareBlockAndFillWithoutKey(
                *first, params->final, first->type != AggregatedDataVariants::Type::without_key);

            setCurrentChunk(convertToChunk(block));
        }
    }

    void mergeSingleLevel()
    {
        AggregatedDataVariantsPtr & first = data->at(0);

        if (current_bucket_num > 0 || first->type == AggregatedDataVariants::Type::without_key)
        {
            finished = true;
            return;
        }

        ++current_bucket_num;

#define M(NAME) \
    else if (first->type == AggregatedDataVariants::Type::NAME) \
        params->aggregator_ext.mergeSingleLevelDataImpl<decltype(first->NAME)::element_type>(*data);
        if (false)
        {
        } // NOLINT
        APPLY_FOR_VARIANTS_SINGLE_LEVEL(M)
#undef M
        else throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant.");

        auto block = params->aggregator_ext.prepareBlockAndFillSingleLevel(*first, params->final);

        setCurrentChunk(convertToChunk(block));
        finished = true;
    }

    void createSources()
    {
        if (num_threads == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "num_threads can't be zero when use two-level aggregation");
        AggregatedDataVariantsPtr & first = data->at(0);
        processors.reserve(num_threads);

        for (size_t thread = 0; thread < num_threads; ++thread)
        {
            /// Select Arena to avoid race conditions
            Arena * arena = first->aggregates_pools.at(thread).get();
            auto source = std::make_shared<ConvertingAggregatedToChunksSourceExt>(params, data, shared_data, arena);

            processors.emplace_back(std::move(source));
        }
    }
};

AggregatingTransformExt::AggregatingTransformExt(Block header, AggregatingTransformParamsExtPtr params_)
    : AggregatingTransformExt(std::move(header), std::move(params_), std::make_unique<ManyAggregatedData>(1), 0, 1, 1)
{
}

AggregatingTransformExt::AggregatingTransformExt(
    Block header,
    AggregatingTransformParamsExtPtr params_,
    ManyAggregatedDataPtr many_data_,
    size_t current_variant,
    size_t max_threads_,
    size_t temporary_data_merge_threads_)
    : IProcessor({std::move(header)}, {params_->getHeader()})
    , params(std::move(params_))
    , key_columns(params->params.keys_size)
    , aggregate_columns(params->params.aggregates_size)
    , many_data(std::move(many_data_))
    , variants(*many_data->variants[current_variant])
    , max_threads(std::min(many_data->variants.size(), max_threads_))
    , temporary_data_merge_threads(temporary_data_merge_threads_)
{
}

void AggregatingTransformParamsExt::toProto(Protos::AggregatingTransformParamsExt & proto) const
{
    params.toProto(*proto.mutable_params());
    proto.set_final(final);
}

std::shared_ptr<AggregatingTransformParamsExt>
AggregatingTransformParamsExt::fromProto(const Protos::AggregatingTransformParamsExt & proto, ContextPtr context)
{
    auto params = AggregatorExt::Params::fromProto(proto.params(), context);
    auto final = proto.final();
    auto step = std::make_shared<AggregatingTransformParamsExt>(params, final);

    return step;
}

AggregatingTransformExt::~AggregatingTransformExt() = default;

IProcessor::Status AggregatingTransformExt::prepare()
{
    /// There are one or two input ports.
    /// The first one is used at aggregation step, the second one - while reading merged data from ConvertingAggregated

    auto & output = outputs.front();
    /// Last output is current. All other outputs should already be closed.
    auto & input = inputs.back();

    /// Check can output.
    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Finish data processing, prepare to generating.
    if (is_consume_finished && !is_generate_initialized)
    {
        /// Close input port in case max_rows_to_group_by was reached but not all data was read.
        inputs.front().close();

        return Status::Ready;
    }

    if (is_generate_initialized && !is_pipeline_created && !processors.empty())
        return Status::ExpandPipeline;

    /// Only possible while consuming.
    if (read_current_chunk)
        return Status::Ready;

    /// Get chunk from input.
    if (input.isFinished())
    {
        if (is_consume_finished)
        {
            output.finish();
            return Status::Finished;
        }
        else
        {
            /// Finish data processing and create another pipe.
            is_consume_finished = true;
            return Status::Ready;
        }
    }

    if (!input.hasData())
    {
        input.setNeeded();
        return Status::NeedData;
    }

    if (is_consume_finished)
        input.setNeeded();

    current_chunk = input.pull(/*set_not_needed = */ !is_consume_finished);
    read_current_chunk = true;

    if (is_consume_finished)
    {
        output.push(std::move(current_chunk));
        read_current_chunk = false;
        return Status::PortFull;
    }

    return Status::Ready;
}

void AggregatingTransformExt::work()
{
    if (is_consume_finished)
        initGenerate();
    else
    {
        consume(std::move(current_chunk));
        read_current_chunk = false;
    }
}

Processors AggregatingTransformExt::expandPipeline()
{
    auto & out = processors.back()->getOutputs().front();
    inputs.emplace_back(out.getHeader(), this);
    connect(out, inputs.back());
    is_pipeline_created = true;
    return std::move(processors);
}

void AggregatingTransformExt::consume(Chunk chunk)
{
    const UInt64 num_rows = chunk.getNumRows();

    if (num_rows == 0 && params->params.empty_result_for_aggregation_by_empty_set)
        return;

    if (!is_consume_started)
    {
        LOG_TRACE(log, "AggregatingExt");
        is_consume_started = true;
    }

    src_rows += num_rows;
    src_bytes += chunk.bytes();

    if (params->only_merge)
    {
        auto block = getInputs().front().getHeader().cloneWithColumns(chunk.detachColumns());
        block = materializeBlock(block);
        if (!params->aggregator_ext.mergeOnBlock(block, variants, no_more_keys, is_cancelled))
            is_consume_finished = true;
    }
    else
    {
        if (!params->aggregator_ext.executeOnBlock(chunk.detachColumns(), num_rows, variants, key_columns, aggregate_columns, no_more_keys))
            is_consume_finished = true;
    }
}

void AggregatingTransformExt::initGenerate()
{
    if (is_generate_initialized)
        return;

    is_generate_initialized = true;

    /// If there was no data, and we aggregate without keys, and we must return single row with the result of empty aggregation.
    /// To do this, we pass a block with zero rows to aggregate.
    if (variants.empty() && params->params.keys_size == 0 && !params->params.empty_result_for_aggregation_by_empty_set)
    {
        if (params->only_merge)
            params->aggregator_ext.mergeOnBlock(getInputs().front().getHeader(), variants, no_more_keys, is_cancelled);
        else
            params->aggregator_ext.executeOnBlock(getInputs().front().getHeader(), variants, key_columns, aggregate_columns, no_more_keys);
    }

    double elapsed_seconds = watch.elapsedSeconds();
    size_t rows = variants.sizeWithoutOverflowRow();

    LOG_DEBUG(
        log,
        "Aggregated. {} to {} rows (from {}) in {} sec. ({:.3f} rows/sec., {}/sec.)",
        src_rows,
        rows,
        ReadableSize(src_bytes),
        elapsed_seconds,
        src_rows / elapsed_seconds,
        ReadableSize(src_bytes / elapsed_seconds));

    if (params->aggregator_ext.hasTemporaryData())
    {
        if (variants.isConvertibleToTwoLevel())
            variants.convertToTwoLevel();

        /// Flush data in the RAM to disk also. It's easier than merging on-disk and RAM data.
        if (!variants.empty())
            params->aggregator_ext.writeToTemporaryFile(variants);
    }

    if (many_data->num_finished.fetch_add(1) + 1 < many_data->variants.size())
        return;

    if (!params->aggregator_ext.hasTemporaryData())
    {
        auto prepared_data = params->aggregator_ext.prepareVariantsToMerge(many_data->variants);
        auto prepared_data_ptr = std::make_shared<ManyAggregatedDataVariants>(std::move(prepared_data));
        processors.emplace_back(
            std::make_shared<ConvertingAggregatedToChunksTransformExt>(params, std::move(prepared_data_ptr), max_threads));
    }
    else
    {
        /// If there are temporary files with partially-aggregated data on the disk,
        /// then read and merge them, spending the minimum amount of memory.

        ProfileEvents::increment(ProfileEvents::ExternalAggregationMerge);

        if (many_data->variants.size() > 1)
        {
            /// It may happen that some data has not yet been flushed,
            ///  because at the time thread has finished, no data has been flushed to disk, and then some were.
            for (auto & cur_variants : many_data->variants)
            {
                if (cur_variants->isConvertibleToTwoLevel())
                    cur_variants->convertToTwoLevel();

                if (!cur_variants->empty())
                    params->aggregator_ext.writeToTemporaryFile(*cur_variants);
            }
        }

        size_t num_streams = 0;
        size_t compressed_size = 0;
        size_t uncompressed_size = 0;

        Pipes pipes;
        /// Merge external data from all aggregators used in query.
        for (const auto & aggregator : *params->aggregator_ext_list_ptr)
        {
            const auto & tmp_data = aggregator.getTemporaryData();
            for (auto * tmp_stream : tmp_data.getStreams())
                pipes.emplace_back(Pipe(std::make_unique<SourceFromNativeStreamExt>(tmp_stream)));

            num_streams += tmp_data.getStreams().size();
            compressed_size += tmp_data.getStat().compressed_size;
            uncompressed_size += tmp_data.getStat().uncompressed_size;
        }

        LOG_DEBUG(
            log,
            "Will merge {} temporary files of size {} compressed, {} uncompressed.",
            num_streams,
            ReadableSize(compressed_size),
            ReadableSize(uncompressed_size));

        auto pipe = Pipe::unitePipes(std::move(pipes));
        addMergingAggregatedMemoryEfficientTransform(pipe, params, temporary_data_merge_threads);

        processors = Pipe::detachProcessors(std::move(pipe));
    }
}

}
