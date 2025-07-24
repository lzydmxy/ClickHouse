#pragma once

#include <Processors/Transforms/AggregatingTransform.h>
#include <Query/Interpreters/AggregatorExt.h>

namespace DB
{

namespace Protos
{
class AggregatingTransformParamsExt;
}

using AggregatorExtList = std::list<AggregatorExt>;
using AggregatorExtListPtr = std::shared_ptr<AggregatorExtList>;

class AggregatedArenasChunkInfo : public ChunkInfo
{
public:
    Arenas arenas;
    AggregatedArenasChunkInfo(Arenas arenas_) : arenas(std::move(arenas_)) { }
};


struct AggregatingTransformParamsExt
{
    AggregatorExt::Params params;
    /// Each params holds a list of aggregators which are used in query. It's needed because we need
    /// to use a pointer of aggregator to proper destroy complex aggregation states on exception
    /// (See comments in AggregatedDataVariants). However, this pointer might not be valid because
    /// we can have two different aggregators at the same time due to mixed pipeline of aggregate
    /// projections, and one of them might gets destroyed before used.
    AggregatorExtListPtr aggregator_ext_list_ptr;
    AggregatorExt & aggregator_ext;
    bool final;
    bool only_merge = false;

    AggregatingTransformParamsExt(const AggregatorExt::Params & params_, bool final_)
        : params(params_)
        , aggregator_ext_list_ptr(std::make_shared<AggregatorExtList>())
        , aggregator_ext(*aggregator_ext_list_ptr->emplace(aggregator_ext_list_ptr->end(), params))
        , final(final_)
    {
    }

    AggregatingTransformParamsExt(const AggregatorExt::Params & params_, const AggregatorExtListPtr & aggregator_ext_list_ptr_, bool final_)
        : params(params_)
        , aggregator_ext_list_ptr(aggregator_ext_list_ptr_)
        , aggregator_ext(*aggregator_ext_list_ptr->emplace(aggregator_ext_list_ptr->end(), params))
        , final(final_)
    {
    }

    Block getHeader() const { return aggregator_ext.getHeader(final); }

    Block getCustomHeader(bool final_) const { return aggregator_ext.getHeader(final_); }

    void toProto(Protos::AggregatingTransformParamsExt & proto) const;
    static std::shared_ptr<AggregatingTransformParamsExt>
    fromProto(const Protos::AggregatingTransformParamsExt & proto, ContextPtr context);
};

using AggregatingTransformParamsExtPtr = std::shared_ptr<AggregatingTransformParamsExt>;
using ManyAggregatedDataPtr = std::shared_ptr<ManyAggregatedData>;

/** Aggregates the stream of blocks using the specified key columns and aggregate functions.
   * Columns with aggregate functions adds to the end of the block.
   * If final = false, the aggregate functions are not finalized, that is, they are not replaced by their value, but contain an intermediate state of calculations.
   * This is necessary so that aggregation can continue (for example, by combining streams of partially aggregated data).
   *
   * For every separate stream of data separate AggregatingTransform is created.
   * Every AggregatingTransform reads data from the first port till is is not run out, or max_rows_to_group_by reached.
   * When the last AggregatingTransform finish reading, the result of aggregation is needed to be merged together.
   * This task is performed by ConvertingAggregatedToChunksTransform.
   * Last AggregatingTransform expands pipeline and adds second input port, which reads from ConvertingAggregated.
   *
   * Aggregation data is passed by ManyAggregatedData structure, which is shared between all aggregating transforms.
   * At aggregation step, every transform uses it's own AggregatedDataVariants structure.
   * At merging step, all structures pass to ConvertingAggregatedToChunksTransform.
   */
class AggregatingTransformExt : public IProcessor
{
public:
    AggregatingTransformExt(Block header, AggregatingTransformParamsExtPtr params_);

    /// For Parallel aggregating.
    AggregatingTransformExt(
        Block header,
        AggregatingTransformParamsExtPtr params_,
        ManyAggregatedDataPtr many_data,
        size_t current_variant,
        size_t max_threads,
        size_t temporary_data_merge_threads);
    ~AggregatingTransformExt() override;

    String getName() const override { return "AggregatingTransformExt"; }
    Status prepare() override;
    void work() override;
    Processors expandPipeline() override;

protected:
    void consume(Chunk chunk);

private:
    /// To read the data that was flushed into the temporary data file.
    Processors processors;

    AggregatingTransformParamsExtPtr params;
    LoggerPtr log = getLogger("AggregatingTransformExt");

    ColumnRawPtrs key_columns;
    AggregatorExt::AggregateColumns aggregate_columns;

    /** Used if there is a limit on the maximum number of rows in the aggregation,
      *   and if group_by_overflow_mode == ANY.
      *  In this case, new keys are not added to the set, but aggregation is performed only by
      *   keys that have already managed to get into the set.
      */
    bool no_more_keys = false;

    ManyAggregatedDataPtr many_data;
    AggregatedDataVariants & variants;
    size_t max_threads = 1;
    size_t temporary_data_merge_threads = 1;

    /// TODO: calculate time only for aggregation.
    Stopwatch watch;

    UInt64 src_rows = 0;
    UInt64 src_bytes = 0;

    bool is_generate_initialized = false;
    bool is_consume_finished = false;
    bool is_pipeline_created = false;

    Chunk current_chunk;
    bool read_current_chunk = false;

    bool is_consume_started = false;

    void initGenerate();
};

}
