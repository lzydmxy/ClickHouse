#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>

#include <Processors/Transforms/AggregatingTransform.h>
#include <Query/Processors/QueryPlan/AggregatingStepExt.h>


namespace DB
{

// FIXME: MergingAggregatedStepExt might be better off inheriting directly from ITransformingStep instead of MergingAggregatedStep.
// Inheriting from MergingAggregatedStep could lead to some errors, because some functionalities of the community versions of
// MergingAggregatedStep and MergingAggregatedStepExt overlap and conflict with each other.
class MergingAggregatedStepExt : public MergingAggregatedStep
{
public:
    MergingAggregatedStepExt(
        const DataStream & input_stream_,
        GroupingSetsParamsExtList grouping_sets_params_,
        GroupingDescriptions groupings_,
        bool final_,
        Aggregator::Params params_,
        bool memory_efficient_aggregation_,
        size_t max_threads_,
        size_t memory_efficient_merge_threads_,
        size_t max_block_size_,
        size_t memory_bound_merging_max_block_bytes_,
        SortDescription group_by_sort_description_,
        bool memory_bound_merging_of_aggregation_results_enabled_);

    String getName() const override { return "MergingAggregatedExt"; }
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const;

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    const Names & getKeys() const { return params.keys; }
    const AggregateDescriptions & getAggregates() const { return params.aggregates; }
    const GroupingDescriptions & getGroupings() const { return groupings; }
    const GroupingSetsParamsExtList & getGroupingSetsParamsList() const { return grouping_sets_params; }
    bool isMemoryEfficientAggregation() const
    {
        return memory_efficient_aggregation;
    }
    size_t getMaxThreads() const { return max_threads; }
    size_t getMaxBlockSize() const { return max_block_size; }
    size_t getMemoryBoundMergingMaxBlockBytes() const { return memory_bound_merging_max_block_bytes; }

    bool isFinal() const { return final; }
    size_t getMemoryEfficientMergeThreads() const { return memory_efficient_merge_threads; }
    bool isShouldProduceResultsInOrderOfBucketNumber() const { return should_produce_results_in_order_of_bucket_number; }
    void setShouldProduceResultsInOrderOfBucketNumber(bool value) { should_produce_results_in_order_of_bucket_number = value; }
    void setMemoryEfficientAggregation(bool value) { memory_efficient_aggregation = value; }
    bool getMemoryBoundMergingOfAggregationResultsEnabled() const {return memory_bound_merging_of_aggregation_results_enabled;}
    const SortDescription & getGroupBySortDescription() const {return group_by_sort_description;}

    void updateOutputStream() override;

    void toProto(Protos::MergingAggregatedStepExt & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<MergingAggregatedStepExt> fromProto(const Protos::MergingAggregatedStepExt & proto, ContextPtr context);
private:
    GroupingSetsParamsExtList grouping_sets_params;
    GroupingDescriptions groupings;
};

}
