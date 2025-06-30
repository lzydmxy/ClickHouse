#pragma once

#include <Columns/ColumnAggregateFunction.h>
#include <Core/Block.h>
#include <Core/ColumnNumbers.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/AggregatedDataVariants.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Query/Protos/EnumMacros.h>
#include <Query/Protos/common.pb.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{

/** How are "total" values calculated with WITH TOTALS?
   * (For more details, see TotalsHavingTransform.)
   *
   * In the absence of group_by_overflow_mode = 'any', the data is aggregated as usual, but the states of the aggregate functions are not finalized.
   * Later, the aggregate function states for all rows (passed through HAVING) are merged into one - this will be TOTALS.
   *
   * If there is group_by_overflow_mode = 'any', the data is aggregated as usual, except for the keys that did not fit in max_rows_to_group_by.
   * For these keys, the data is aggregated into one additional row - see below under the names `overflow_row`, `overflows`...
   * Later, the aggregate function states for all rows (passed through HAVING) are merged into one,
   *  also overflow_row is added or not added (depending on the totals_mode setting) also - this will be TOTALS.
   */

/** Aggregates the source of the blocks.
   */
namespace Protos
{
class AggregatorExtParams;
}

/// What to do if the limit is exceeded.
ENUM_TO_PROTO_CONVERTER(
    OverflowMode, // enum name
    Protos::OverflowMode, // proto enum message
    (THROW, 0), /// Throw exception.
    (BREAK, 1), /// Abort query execution, return what is.
    /** Only for GROUP BY: do not add new rows to the set,
      * but continue to aggregate for keys that are already in the set.
      */
    (ANY, 2));

/// TODO: lizhuoyu, should use Aggregator to replace AggregatorExt in order to be compatible with some optimizations and adjustments to aggregation in the new version.
class AggregatorExt final
{
public:
    struct Params
    {
        using StatsCollectingParams = Aggregator::Params::StatsCollectingParams;

        enum class TwoLevelMode
        {
            ADAPTIVE,
            ENFORCE_SINGLE_LEVEL,
            ENFORCE_TWO_LEVEL,
        };

        /// Data structure of source blocks.
        Block src_header;
        /// Data structure of intermediate blocks before merge.
        Block intermediate_header;

        /// What to count.
        const ColumnNumbers keys;
        const AggregateDescriptions aggregates;
        const size_t keys_size;
        const size_t aggregates_size;

        /// The settings of approximate calculation of GROUP BY.
        const bool
            overflow_row; /// Do we need to put into AggregatedDataVariants::without_key aggregates for keys that are not in max_rows_to_group_by.
        const size_t max_rows_to_group_by;
        const OverflowMode group_by_overflow_mode;

        /// Two-level aggregation settings (used for a large number of keys).
        /** With how many keys or the size of the aggregation state in bytes,
           *  two-level aggregation begins to be used. Enough to reach of at least one of the thresholds.
           * 0 - the corresponding threshold is not specified.
           */
        size_t group_by_two_level_threshold;
        size_t group_by_two_level_threshold_bytes;

        /// Settings to flush temporary data to the filesystem (external aggregation).
        const size_t max_bytes_before_external_group_by; /// 0 - do not use external aggregation.

        const bool enable_adaptive_spill;

        /// Control the size of the memory in the agg stage when flushing the disk (external aggregation).
        const size_t spill_buffer_bytes_before_external_group_by;

        /// Return empty result when aggregating without keys on empty set.
        bool empty_result_for_aggregation_by_empty_set;

        TemporaryDataOnDiskScopePtr tmp_data_scope;

        /// Settings is used to determine cache size. No threads are created.
        size_t max_threads;

        const size_t min_free_disk_space;

        bool compile_aggregate_expressions;
        size_t min_count_to_compile_aggregate_expression;

        size_t max_block_size;
        bool only_merge;

        bool enable_prefetch;

        bool optimize_group_by_constant_keys;

        const double min_hit_rate_to_use_consecutive_keys_optimization;

        StatsCollectingParams stats_collecting_params;

        // this field is determined when build pipeline, thus it doesn't need to be serialized.
        TwoLevelMode two_level_mode = TwoLevelMode::ADAPTIVE;

        const bool enable_lc_group_by_opt;

        Params(
            const Block & src_header_,
            const ColumnNumbers & keys_,
            const AggregateDescriptions & aggregates_,
            bool overflow_row_,
            size_t max_rows_to_group_by_,
            OverflowMode group_by_overflow_mode_,
            size_t group_by_two_level_threshold_,
            size_t group_by_two_level_threshold_bytes_,
            size_t max_bytes_before_external_group_by_,
            bool enable_adaptive_spill_,
            size_t spill_buffer_bytes_before_external_group_by_,
            bool empty_result_for_aggregation_by_empty_set_,
            TemporaryDataOnDiskScopePtr tmp_data_scope_,
            size_t max_threads_,
            size_t min_free_disk_space_,
            bool compile_aggregate_expressions_,
            size_t min_count_to_compile_aggregate_expression_,
            size_t max_block_size_,
            bool enable_prefetch_,
            bool only_merge_, // true for projections
            bool optimize_group_by_constant_keys_,
            double min_hit_rate_to_use_consecutive_keys_optimization_,
            const StatsCollectingParams & stats_collecting_params_,
            const Block & intermediate_header_ = {},
            bool enable_lc_group_by_opt_ = false)
            : src_header(src_header_)
            , intermediate_header(intermediate_header_)
            , keys(keys_)
            , aggregates(aggregates_)
            , keys_size(keys.size())
            , aggregates_size(aggregates.size())
            , overflow_row(overflow_row_)
            , max_rows_to_group_by(max_rows_to_group_by_)
            , group_by_overflow_mode(group_by_overflow_mode_)
            , group_by_two_level_threshold(group_by_two_level_threshold_)
            , group_by_two_level_threshold_bytes(group_by_two_level_threshold_bytes_)
            , max_bytes_before_external_group_by(max_bytes_before_external_group_by_)
            , enable_adaptive_spill(enable_adaptive_spill_)
            , spill_buffer_bytes_before_external_group_by(spill_buffer_bytes_before_external_group_by_)
            , empty_result_for_aggregation_by_empty_set(empty_result_for_aggregation_by_empty_set_)
            , tmp_data_scope(std::move(tmp_data_scope_))
            , max_threads(max_threads_)
            , min_free_disk_space(min_free_disk_space_)
            , compile_aggregate_expressions(compile_aggregate_expressions_)
            , min_count_to_compile_aggregate_expression(min_count_to_compile_aggregate_expression_)
            , max_block_size(max_block_size_)
            , only_merge(only_merge_)
            , enable_prefetch(enable_prefetch_)
            , optimize_group_by_constant_keys(optimize_group_by_constant_keys_)
            , min_hit_rate_to_use_consecutive_keys_optimization(min_hit_rate_to_use_consecutive_keys_optimization_)
            , stats_collecting_params(stats_collecting_params_)
            , enable_lc_group_by_opt(enable_lc_group_by_opt_)
        {
        }

        /// Only parameters that matter during merge.
        Params(
            const Block & intermediate_header_,
            const ColumnNumbers & keys_,
            const AggregateDescriptions & aggregates_,
            bool overflow_row_,
            size_t max_threads_,
            bool min_hit_rate_to_use_consecutive_keys_optimization_)
            : Params(
                  Block(),
                  keys_,
                  aggregates_,
                  overflow_row_,
                  0,
                  OverflowMode::THROW,
                  0,
                  0,
                  0,
                  false,
                  10485760,
                  false,
                  nullptr,
                  max_threads_,
                  0,
                  false,
                  0,
                  0,
                  false,
                  true,
                  false,
                  min_hit_rate_to_use_consecutive_keys_optimization_,
                  {},
                  {},
                  false)
        {
            intermediate_header = intermediate_header_;
        }

        static Block getHeader(
            const Block & src_header,
            const Block & intermediate_header,
            const ColumnNumbers & keys,
            const AggregateDescriptions & aggregates,
            bool final);

        Block getHeader(bool final) const { return getHeader(src_header, intermediate_header, keys, aggregates, final); }

        /// Returns keys and aggregated for EXPLAIN query
        void explain(WriteBuffer & out, size_t indent) const;
        void explain(JSONBuilder::JSONMap & map) const;

        void toProto(Protos::AggregatorExtParams & proto) const;
        static AggregatorExt::Params fromProto(const Protos::AggregatorExtParams & proto, ContextPtr context);
    };

    /// Only part of the params that required for ChooseMethod
    struct ChooseMethodOption
    {
        const Block & header;
        const ColumnNumbers & keys;
        bool enable_lc_group_by_opt;
    };

    explicit AggregatorExt(const Params & params_);

    using AggregateColumns = std::vector<ColumnRawPtrs>;
    using AggregateColumnsData = std::vector<ColumnAggregateFunction::Container *>;
    using AggregateColumnsConstData = std::vector<const ColumnAggregateFunction::Container *>;
    using AggregateFunctionsPlainPtrs = std::vector<const IAggregateFunction *>;

    /// Process one block. Return false if the processing should be aborted (with group_by_overflow_mode = 'break').
    bool executeOnBlock(
        const Block & block,
        AggregatedDataVariants & result,
        ColumnRawPtrs & key_columns,
        AggregateColumns & aggregate_columns, /// Passed to not create them anew for each block
        bool & no_more_keys) const;

    bool executeOnBlock(
        Columns columns,
        UInt64 num_rows,
        AggregatedDataVariants & result,
        ColumnRawPtrs & key_columns,
        AggregateColumns & aggregate_columns, /// Passed to not create them anew for each block
        bool & no_more_keys) const;

    /// Used for aggregate projection.
    bool mergeOnBlock(Block block, AggregatedDataVariants & result, bool & no_more_keys, std::atomic<bool> & is_cancelled) const;


    /** Convert the aggregation data structure into a block.
       * If overflow_row = true, then aggregates for rows that are not included in max_rows_to_group_by are put in the first block.
       *
       * If final = false, then ColumnAggregateFunction is created as the aggregation columns with the state of the calculations,
       *  which can then be combined with other states (for distributed query processing).
       * If final = true, then columns with ready values are created as aggregate columns.
       */
    BlocksList convertToBlocks(AggregatedDataVariants & data_variants, bool final, size_t max_threads) const;

    ManyAggregatedDataVariants prepareVariantsToMerge(ManyAggregatedDataVariants & data_variants) const;

    using BucketToBlocks = std::map<Int32, BlocksList>;
    /// Merge partially aggregated blocks separated to buckets into one data structure.
    void
    mergeBlocks(BucketToBlocks bucket_to_blocks, AggregatedDataVariants & result, size_t max_threads, std::atomic<bool> & is_cancelled);

    /// Merge several partially aggregated blocks into one.
    /// Precondition: for all blocks block.info.is_overflows flag must be the same.
    /// (either all blocks are from overflow data or none blocks are).
    /// The resulting block has the same value of is_overflows flag.
    Block mergeBlocks(BlocksList & blocks, bool final, std::atomic<bool> & is_cancelled);

    /** Split block with partially-aggregated data to many blocks, as if two-level method of aggregation was used.
       * This is needed to simplify merging of that data with other results, that are already two-level.
       */
    std::vector<Block> convertBlockToTwoLevel(const Block & block) const;

    /// For external aggregation.
    void writeToTemporaryFile(AggregatedDataVariants & data_variants, size_t max_temp_file_size = 0) const;

    bool hasTemporaryData() const { return tmp_data && !tmp_data->empty(); }

    const TemporaryDataOnDisk & getTemporaryData() const { return *tmp_data; }

    /// Get data structure of the result.
    Block getHeader(bool final) const;

    void turnOnAggStreaming() { is_agg_streaming = true; }
    void turnOffAggStreaming() { is_agg_streaming = false; }

    void turnOnAggConvertingForCache() { is_agg_converting_for_cache = true; }

    bool isWithoutKey() const { return method_chosen == AggregatedDataVariants::Type::without_key; }

    static void
    chooseAggregationMethodByOption(const ChooseMethodOption & option, Sizes & key_sizes, AggregatedDataVariants::Type & method_chosen);

private:
    friend struct AggregatedDataVariants;
    friend class ConvertingAggregatedToChunksTransformExt;
    friend class ConvertingAggregatedToChunksSourceExt;
    friend class AggregatingInOrderTransformExt;
    friend class AggregatingStreamingTransformExt;
    friend class MergingAggregatedStreamingTransform;
    friend class PreAggregatingTransform;

    Params params;

    bool is_agg_streaming = false;
    bool is_agg_converting_for_cache = false;

    AggregatedDataVariants::Type method_chosen;
    Sizes key_sizes;

    HashMethodContextPtr aggregation_state_cache;

    AggregateFunctionsPlainPtrs aggregate_functions;

    /** This array serves two purposes.
       *
       * Function arguments are collected side by side, and they do not need to be collected from different places. Also the array is made zero-terminated.
       * The inner loop (for the case without_key) is almost twice as compact; performance gain of about 30%.
       */
    struct AggregateFunctionInstruction
    {
        const IAggregateFunction * that{};
        size_t state_offset{};
        const IColumn ** arguments{};
        const IAggregateFunction * batch_that{};
        const IColumn ** batch_arguments{};
        const UInt64 * offsets{};
    };

    using AggregateFunctionInstructions = std::vector<AggregateFunctionInstruction>;
    using NestedColumnsHolder = std::vector<std::vector<const IColumn *>>;

    Sizes offsets_of_aggregate_states; /// The offset to the n-th aggregate function in a row of aggregate functions.
    size_t total_size_of_aggregate_states = 0; /// The total size of the row from the aggregate functions.

    // add info to track alignment requirement
    // If there are states whose alignment are v1, ..vn, align_aggregate_states will be max(v1, ... vn)
    size_t align_aggregate_states = 1;

    bool all_aggregates_has_trivial_destructor = false;

    /// How many RAM were used to process the query before processing the first block.
    Int64 memory_usage_before_aggregation = 0;

    LoggerPtr log = getLogger("AggregatorExt");

    /// For external aggregation.
    TemporaryDataOnDiskPtr tmp_data;

    constexpr static const double large_midstate_estimate_by_input_ratio = 10.0;

    mutable size_t delta_bytes_of_large_midstate_agg_inputs = 0;

    mutable bool spilled = false;

#if USE_EMBEDDED_COMPILER
    std::shared_ptr<CompiledAggregateFunctionsHolder> compiled_aggregate_functions_holder;
#endif

    std::vector<bool> is_aggregate_function_compiled;

    /** Try to compile aggregate functions.
       */
    void compileAggregateFunctions();

    /** Select the aggregation method based on the number and types of keys. */
    AggregatedDataVariants::Type chooseAggregationMethod();

    /** Create states of aggregate functions for one key.
       */
    template <bool skip_compiled_aggregate_functions = false>
    void createAggregateStates(AggregateDataPtr & aggregate_data) const;

    /** Call `destroy` methods for states of aggregate functions.
       * Used in the exception handler for aggregation, since RAII in this case is not applicable.
       */
    void destroyAllAggregateStates(AggregatedDataVariants & result) const;


    /// Process one data block, aggregate the data into a hash table.
    template <typename Method>
    void executeImpl(
        Method & method,
        Arena * aggregates_pool,
        size_t rows,
        ColumnRawPtrs & key_columns,
        AggregateFunctionInstruction * aggregate_instructions,
        bool no_more_keys,
        AggregateDataPtr overflow_row) const;

    /// Specialization for a particular value no_more_keys.
    template <bool no_more_keys, bool use_compiled_expressions, typename Method>
    void executeImplBatch(
        Method & method,
        typename Method::State & state,
        Arena * aggregates_pool,
        size_t rows,
        AggregateFunctionInstruction * aggregate_instructions,
        AggregateDataPtr overflow_row) const;

    /// For case when there are no keys (all aggregate into one row).
    static void executeWithoutKeyImpl(
        AggregatedDataWithoutKey & res, size_t rows, AggregateFunctionInstruction * aggregate_instructions, Arena * arena);

    static void executeOnIntervalWithoutKeyImpl(
        AggregatedDataWithoutKey & res,
        size_t row_begin,
        size_t row_end,
        AggregateFunctionInstruction * aggregate_instructions,
        Arena * arena);

    template <typename Method>
    void writeToTemporaryFileImpl(AggregatedDataVariants & data_variants, Method & method, TemporaryFileStream & out) const;

    /// Merge NULL key data from hash table `src` into `dst`.
    template <typename Method, typename Table>
    void mergeDataNullKey(Table & table_dst, Table & table_src, Arena * arena) const;

    /// Merge data from hash table `src` into `dst`.
    template <typename Method, typename Table>
    void mergeDataImpl(Table & table_dst, Table & table_src, Arena * arena, bool use_compiled_functions, bool prefetch) const;

    /// Merge data from hash table `src` into `dst`, but only for keys that already exist in dst. In other cases, merge the data into `overflows`.
    template <typename Method, typename Table>
    void mergeDataNoMoreKeysImpl(Table & table_dst, AggregatedDataWithoutKey & overflows, Table & table_src, Arena * arena) const;

    /// Same, but ignores the rest of the keys.
    template <typename Method, typename Table>
    void mergeDataOnlyExistingKeysImpl(Table & table_dst, Table & table_src, Arena * arena) const;

    void mergeWithoutKeyDataImpl(ManyAggregatedDataVariants & non_empty_data) const;

    template <typename Method>
    void mergeSingleLevelDataImpl(ManyAggregatedDataVariants & non_empty_data) const;

    template <typename Method, typename Table>
    void convertToBlockImpl(
        Method & method,
        Table & data,
        MutableColumns & key_columns,
        AggregateColumnsData & aggregate_columns,
        MutableColumns & final_aggregate_columns,
        Arena * arena,
        bool final) const;

    template <typename Mapped>
    void insertAggregatesIntoColumns(Mapped & mapped, MutableColumns & final_aggregate_columns, Arena * arena) const;

    template <typename Method, bool use_compiled_functions, typename Table>
    void convertToBlockImplFinal(
        Method & method, Table & data, std::vector<IColumn *> key_columns, MutableColumns & final_aggregate_columns, Arena * arena) const;

    template <typename Method, typename Table>
    void convertToBlockImplNotFinal(
        Method & method, Table & data, std::vector<IColumn *> key_columns, AggregateColumnsData & aggregate_columns) const;

    template <typename Filler>
    Block prepareBlockAndFill(AggregatedDataVariants & data_variants, bool final, size_t rows, Filler && filler) const;

    template <typename Method>
    Block convertOneBucketToBlock(AggregatedDataVariants & data_variants, Method & method, Arena * arena, bool final, size_t bucket) const;

    Block mergeAndConvertOneBucketToBlock(
        ManyAggregatedDataVariants & variants, Arena * arena, bool final, size_t bucket, std::atomic<bool> * is_cancelled = nullptr) const;

    Block prepareBlockAndFillWithoutKey(AggregatedDataVariants & data_variants, bool final, bool is_overflows) const;
    Block prepareBlockAndFillSingleLevel(AggregatedDataVariants & data_variants, bool final) const;
    BlocksList prepareBlocksAndFillTwoLevel(AggregatedDataVariants & data_variants, bool final, ThreadPool * thread_pool) const;

    template <typename Method>
    BlocksList
    prepareBlocksAndFillTwoLevelImpl(AggregatedDataVariants & data_variants, Method & method, bool final, ThreadPool * thread_pool) const;

    template <bool no_more_keys, typename Method, typename Table>
    void mergeStreamsImplCase(Block & block, Arena * aggregates_pool, Method & method, Table & data, AggregateDataPtr overflow_row) const;

    template <typename Method, typename Table>
    void mergeStreamsImpl(
        Block & block, Arena * aggregates_pool, Method & method, Table & data, AggregateDataPtr overflow_row, bool no_more_keys) const;

    void mergeWithoutKeyStreamsImpl(Block & block, AggregatedDataVariants & result, std::atomic<bool> & is_cancelled) const;

    template <typename Method>
    void mergeBucketImpl(ManyAggregatedDataVariants & data, Int32 bucket, Arena * arena, std::atomic<bool> * is_cancelled = nullptr) const;

    template <typename Method>
    void convertBlockToTwoLevelImpl(
        Method & method, Arena * pool, ColumnRawPtrs & key_columns, const Block & source, std::vector<Block> & destinations) const;

    template <typename Method, typename Table>
    void destroyImpl(Table & table) const;

    void destroyWithoutKey(AggregatedDataVariants & result) const;


    /** Checks constraints on the maximum number of keys for aggregation.
       * If it is exceeded, then, depending on the group_by_overflow_mode, either
       * - throws an exception;
       * - returns false, which means that execution must be aborted;
       * - sets the variable no_more_keys to true.
       */
    bool checkLimits(size_t result_size, bool & no_more_keys) const;

    void prepareAggregateInstructions(
        Columns columns,
        AggregateColumns & aggregate_columns,
        Columns & materialized_columns,
        AggregateFunctionInstructions & instructions,
        NestedColumnsHolder & nested_columns_holder) const;

    void addSingleKeyToAggregateColumns(const AggregatedDataVariants & data_variants, MutableColumns & aggregate_columns) const;

    void addArenasToAggregateColumns(const AggregatedDataVariants & data_variants, MutableColumns & aggregate_columns) const;

    void createStatesAndFillKeyColumnsWithSingleKey(
        AggregatedDataVariants & data_variants, Columns & key_columns, size_t key_row, MutableColumns & final_key_columns) const;
};

class AggregatorExtHelper
{
public:
    static bool isSmallKeys(const AggregatedDataVariants & aggregated_data_variants)
    {
#define APPLY_FOR_VARIANTS_SMALL_KEYS(M) \
    M(key8) \
    M(key16) \
    M(keys16)

        switch (aggregated_data_variants.type)
        {
#define M(NAME) \
    case AggregatedDataVariants::Type::NAME: \
        return true;

            APPLY_FOR_VARIANTS_SMALL_KEYS(M)
#undef M
            default:
                return false;
        }
#undef APPLY_FOR_VARIANTS_SMALL_KEYS
    }

    static size_t getVariantsBufferSizeInBytes(const AggregatedDataVariants & aggregated_data_variants)
    {
        switch (aggregated_data_variants.type)
        {
            case AggregatedDataVariants::Type::EMPTY:
                return 0;
            case AggregatedDataVariants::Type::without_key:
                return 1;

#define M(NAME, IS_TWO_LEVEL) \
    case AggregatedDataVariants::Type::NAME: \
        return aggregated_data_variants.NAME->data.getBufferSizeInBytes();
                APPLY_FOR_AGGREGATED_VARIANTS(M)
#undef M
        }

        __builtin_unreachable();
    }
};

static const double kDefaultSpillTrigerThreshold = 0.7;

}
