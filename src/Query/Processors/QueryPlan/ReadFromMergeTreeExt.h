#pragma once

#include <unordered_set>

#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

using Roaring = roaring::Roaring;
using ImmutableDeleteBitmapPtr = std::shared_ptr<const Roaring>;
using DeleteBitmapGetter = std::function<ImmutableDeleteBitmapPtr(const DataPartPtr &)>;


struct MergeTreeDataSelectAnalysisResult
{
    std::variant<std::exception_ptr, ReadFromMergeTree::AnalysisResult> result;

    bool error() const;
    size_t marks() const;
};

using MergeTreeDataSelectAnalysisResultPtr = std::shared_ptr<MergeTreeDataSelectAnalysisResult>;

class ReadFromMergeTreeExt final : public ReadFromMergeTree
{
public:
    ReadFromMergeTreeExt(
        MergeTreeData::DataPartsVector parts_,
        std::vector<AlterConversionsPtr> alter_conversions_,
        Names all_column_names_,
        const MergeTreeData & data_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        size_t max_block_size_,
        size_t num_streams_,
        std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read_,
        LoggerPtr log_,
        AnalysisResultPtr analyzed_result_ptr_,
        bool enable_parallel_reading_,
        DeleteBitmapGetter delete_bitmap_getter_,
        const size_t min_block_size_,
        const bool size_predictor_estimate_lc_size_by_fullstate_,
        const bool map_column_keys_column_queried_
    );

    String getName() const override { return "ReadFromMergeTreeExt"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    std::shared_ptr<IQueryPlanStep> copy() const;

public:
    DeleteBitmapGetter delete_bitmap_getter;
    const size_t min_block_size;
    const bool size_predictor_estimate_lc_size_by_fullstate;
    const bool map_column_keys_column_queried;
};

}
