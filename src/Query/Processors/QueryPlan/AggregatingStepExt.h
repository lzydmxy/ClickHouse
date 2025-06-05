#pragma once

#include <Core/Names.h>
#include <Query/Interpreters/AggregatorExt.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/SelectQueryInfo.h>
#include <Core/SortDescription.h>

namespace DB
{

bool hasNonParallelAggregateFunctions(const AggregateDescriptions &);

struct GroupingSetsParamsExt
{
    GroupingSetsParamsExt() = default;

    explicit GroupingSetsParamsExt(Names used_key_names_) : used_key_names(std::move(used_key_names_)) { }

    GroupingSetsParamsExt(ColumnNumbers used_keys_, ColumnNumbers missing_keys_)
        : used_keys(std::move(used_keys_)), missing_keys(std::move(missing_keys_))
    {
    }

    Names used_key_names;

    ColumnNumbers used_keys;
    ColumnNumbers missing_keys;

public:
    void toProto(Protos::GroupingSetsParamsExt & proto) const;
    void fillFromProto(const Protos::GroupingSetsParamsExt & proto);
};

using GroupingSetsParamsExtList = std::vector<GroupingSetsParamsExt>;

struct GroupingDescription
{
    Names argument_names;
    String output_name;

public:
    void toProto(Protos::GroupingDescription & proto) const;
    void fillFromProto(const Protos::GroupingDescription & proto);
};

using GroupingDescriptions = std::vector<GroupingDescription>;

/// AggregateStagePolicy represents partition requirements for aggregate node
///
/// 1 single, means insert a gather exchange node before aggregate node,
/// 2 perfect_shard, means the aggregate node match any partition requirement.
/// 3 merge_perfect_shard, needs insert a gather exchange before aggregate.
/// 4 default, in this case, use property enforcement to derive required exchange.
enum class AggregateStagePolicy : UInt8
{
    SINGLE = 0,
    PERFECT_SHARD,
    MERGE_PERFECT_SHARD,
    DEFAULT,
    STATE,
    MERGE
};

void computeGroupingFunctions(
    QueryPipelineBuilder & pipeline,
    const GroupingDescriptions & groupings,
    const Names & keys,
    const GroupingSetsParamsExtList & grouping_set_params,
    const BuildQueryPipelineSettings & build_settings);

/// AggregationExt. See AggregatingTransformExt.
class AggregatingStepExt : public ITransformingStep
{
public:
    friend class QueryPlanStepHelper;

    AggregatingStepExt(
        const DataStream & input_stream_,
        AggregatorExt::Params params_,
        const NameSet & keys_not_hashed_,
        GroupingSetsParamsExtList grouping_sets_params_,
        bool final_,
        AggregateStagePolicy stage_policy_,
        size_t max_block_size_,
        size_t merge_threads_,
        size_t temporary_data_merge_threads_,
        bool storage_has_evenly_distributed_read_,
        InputOrderInfoPtr group_by_info_,
        SortDescriptionWithPositions group_by_sort_description_,
        bool should_produce_results_in_order_of_bucket_number_,
        bool no_shuffle_ = false)
        : AggregatingStepExt(
              input_stream_,
              Names(),
              keys_not_hashed_,
              std::move(params_),
              std::move(grouping_sets_params_),
              final_,
              stage_policy_,
              max_block_size_,
              merge_threads_,
              temporary_data_merge_threads_,
              storage_has_evenly_distributed_read_,
              std::move(group_by_info_),
              std::move(group_by_sort_description_),
              {},
              false,
              should_produce_results_in_order_of_bucket_number_,
              no_shuffle_,
              false)
    {
    }

    AggregatingStepExt(
        const DataStream & input_stream_,
        Names keys_,
        const NameSet & keys_not_hashed_,
        AggregateDescriptions aggregates_,
        GroupingSetsParamsExtList grouping_sets_params_,
        bool final_,
        AggregateStagePolicy stage_policy_ = AggregateStagePolicy::DEFAULT,
        SortDescriptionWithPositions group_by_sort_description_ = {},
        GroupingDescriptions groupings_ = {},
        bool overflow_row_ = false,
        bool should_produce_results_in_order_of_bucket_number_ = false,
        bool no_shuffle_ = false,
        bool streaming_for_cache_ = false)
        : AggregatingStepExt(
              input_stream_,
              keys_,
              keys_not_hashed_,
              createParams(input_stream_.header, aggregates_, keys_, overflow_row_),
              std::move(grouping_sets_params_),
              final_,
              stage_policy_,
              0,
              0,
              0,
              false,
              nullptr,
              group_by_sort_description_,
              groupings_,
              false,
              should_produce_results_in_order_of_bucket_number_,
              no_shuffle_,
              streaming_for_cache_)
    {
    }

    AggregatingStepExt(
        const DataStream & input_stream_,
        Names keys_,
        const NameSet & keys_not_hashed_,
        AggregatorExt::Params params_,
        GroupingSetsParamsExtList grouping_sets_params_,
        bool final_,
        AggregateStagePolicy stage_policy_,
        size_t max_block_size_,
        size_t merge_threads_,
        size_t temporary_data_merge_threads_,
        bool storage_has_evenly_distributed_read_,
        InputOrderInfoPtr group_by_info_,
        SortDescriptionWithPositions group_by_sort_description_,
        GroupingDescriptions groupings_ = {},
        bool totals_ = false,
        bool should_produce_results_in_order_of_bucket_number = true,
        bool no_shuffle_ = false,
        bool streaming_for_cache_ = false);

    static Block appendGroupingColumn(Block block, bool has_grouping);

    String getName() const override { return "AggregatingExt"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;

    void describeActions(FormatSettings &) const override;
    void describePipeline(FormatSettings & settings) const override;

    const AggregatorExt::Params & getParams() const { return params; }
    const AggregateDescriptions & getAggregates() const { return params.aggregates; }
    const Names & getKeys() const { return keys; }
    const NameSet & getKeysNotHashed() const { return keys_not_hashed; }
    const GroupingSetsParamsExtList & getGroupingSetsParams() const { return grouping_sets_params; }
    const SortDescriptionWithPositions & getGroupBySortDescription() const { return group_by_sort_description; }
    void setGroupBySortDescription(const SortDescriptionWithPositions & group_by_sort_description_)
    {
        group_by_sort_description = group_by_sort_description_;
    }
    bool isFinal() const { return final; }
    bool isStreamingForCache() const { return streaming_for_cache; }
    void setStreamingForCache(bool streaming_for_cache_) { streaming_for_cache = streaming_for_cache_; }

    bool isPartial() const { return !final; }
    bool isGroupingSet() const { return !grouping_sets_params.empty(); }
    bool isNoShuffle() const { return no_shuffle; }
    void setNoShuffle(bool no_shuffle_) { no_shuffle = no_shuffle_; }

    size_t getMaxBlockSize() const { return max_block_size; }

    const GroupingDescriptions & getGroupings() const { return groupings; }
    bool shouldProduceResultsInOrderOfBucketNumber() const { return should_produce_results_in_order_of_bucket_number; }
    void setShouldProduceResultsInOrderOfBucketNumber(bool value) { should_produce_results_in_order_of_bucket_number = value; }
    bool needOverflowRow() const { return params.overflow_row; }
    bool isNormal() const { return final && !isGroupingSet() /*&& !totals && !having*/ && groupings.empty(); }

    AggregateStagePolicy getStagePolicy() const { return stage_policy; }
    void setStagePolicy(AggregateStagePolicy policy) { stage_policy = policy; }

    void toProto(Protos::AggregatingStepExt & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<AggregatingStepExt> fromProto(const Protos::AggregatingStepExt & proto, ContextPtr context);

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const;

    void updateOutputStream() override;
    static AggregatorExt::Params
    createParams(Block header_before_aggregation, AggregateDescriptions aggregates, Names group_by_keys, bool overflow_row);
    GroupingSetsParamsExtList prepareGroupingSetsParams() const;

private:
    LoggerPtr log = getLogger("TableScanStepExt");
    Names keys;

    NameSet keys_not_hashed; // keys which can be output directly, same as function `any`, but no type loss.

    AggregatorExt::Params params;
    GroupingSetsParamsExtList grouping_sets_params;
    bool final;

    /// stage_policy field doesn't need be serialize/deserialize
    AggregateStagePolicy stage_policy;

    size_t max_block_size;
    size_t merge_threads;
    size_t temporary_data_merge_threads;

    bool storage_has_evenly_distributed_read;

    InputOrderInfoPtr group_by_info;
    SortDescriptionWithPositions group_by_sort_description;

    GroupingDescriptions groupings;
    /// It determines if we should resize pipeline to 1 at the end.
    /// Needed in case of distributed memory efficient aggregation over distributed table.
    /// Specifically, if there is a further MergingAggregatedStep and
    /// distributed_aggregation_memory_efficient=true
    /// then the pipeline should not be resized to > 1; otherwise,
    /// the data passed to GroupingAggregatedTransform are not in bucket order -> error.
    /// Set as to_stage==WithMergeableState && distributed_aggregation_memory_efficient
    /// which is equivalent to !final_ && && distributed_aggregation_memory_efficient
    /// distributed_aggregation_memory_efficient is not available inside this class,
    /// therefore, this variable is passed to the constructor.
    /// if the condition is unkown, the safe options is to set it to true to avoid errors
    /// therefore the default value is true in the constructor
    bool should_produce_results_in_order_of_bucket_number;
    bool streaming_for_cache = false;

    // for bitengine sqls
    bool no_shuffle;

    Processors aggregating_in_order;
    Processors aggregating_sorted;
    Processors finalizing;

    Processors aggregating;
};

}
