#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>
#include <Interpreters/Context.h>

namespace DB
{

/// Sorts stream of data. See MergeSortingTransform.
class MergeSortingStepExt : public ITransformingStep
{
public:
    explicit MergeSortingStepExt(
            const DataStream & input_stream,
            const SortDescription & description_,
            size_t max_merged_block_size_,
            UInt64 limit_,
            size_t max_bytes_before_remerge_,
            double remerge_lowered_memory_bytes_ratio_,
            size_t max_bytes_before_external_sort_,
            TemporaryDataOnDiskScopePtr tmp_data_,
            size_t min_free_disk_space_);

    explicit MergeSortingStepExt(const DataStream & input_stream, const SortDescription & description_, UInt64 limit_)
        : MergeSortingStepExt(input_stream, description_, 0, limit_, 0, 0, 0, nullptr, 0)
    {
    }

    String getName() const override { return "MergeSortingExt"; }

    const SortDescription & getSortDescription() const { return description; }
    UInt64 getLimit() const { return limit; }
    size_t getMaxMergedBlockSize() const { return max_merged_block_size; }
    size_t getMaxBytesBeforeRemerge() const { return max_bytes_before_remerge; }
    double getRemergeLoweredMemoryBytesRatio() const { return remerge_lowered_memory_bytes_ratio; }
    size_t getMaxBytesBeforeExternalSort() const { return max_bytes_before_external_sort; }
    TemporaryDataOnDiskScopePtr getTmpData() const { return tmp_data; }
    size_t getMinFreeDiskSpace() const { return min_free_disk_space; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    /// Add limit or change it to lower value.
    void updateLimit(size_t limit_);

    void toProto(Protos::MergeSortingStepExt & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<MergeSortingStepExt> fromProto(const Protos::MergeSortingStepExt & proto, ContextPtr context);
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const;
    void setInputStreams(const DataStreams & input_streams_);
    void updateOutputStream() override;

private:
    SortDescription description;
    size_t max_merged_block_size;
    UInt64 limit;

    size_t max_bytes_before_remerge;
    double remerge_lowered_memory_bytes_ratio;
    size_t max_bytes_before_external_sort;
    TemporaryDataOnDiskScopePtr tmp_data;
    size_t min_free_disk_space;
    // bool enable_adaptive_spill; // diff byconity has the feature
};

}

