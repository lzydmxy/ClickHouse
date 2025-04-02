#pragma once

#include <unordered_set>
#include <absl/container/flat_hash_set.h>

#include <Common/escapeForFileName.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Processors/LimitTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Query/Common/MapHelpers.h>
#include <Query/Common/OptimizerContext.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>


namespace DB
{

using Roaring = roaring::Roaring;
using ImmutableDeleteBitmapPtr = std::shared_ptr<const Roaring>;
using DeleteBitmapGetter = std::function<ImmutableDeleteBitmapPtr(const DataPartPtr &)>;


struct MergeTreeDataSelectAnalysisResult
{
    std::variant<std::exception_ptr, ReadFromMergeTree::AnalysisResult> result;

    bool error() const
    {
        return std::holds_alternative<std::exception_ptr>(result);
    }
    size_t marks() const
    {
        if (std::holds_alternative<std::exception_ptr>(result))
            std::rethrow_exception(std::get<std::exception_ptr>(result));

        const auto & index_stats = std::get<ReadFromMergeTree::AnalysisResult>(result).index_stats;
        if (index_stats.empty())
            return 0;
        return index_stats.back().num_granules_after;
    }
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
        const bool map_column_keys_column_queried_,
        const bool sample_factor_column_queried_
    );

    String getName() const override { return "ReadFromMergeTreeExt"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    std::shared_ptr<IQueryPlanStep> copy() const;
    void fillRuntimeAttributeDescriptions(const ReadFromMergeTree::AnalysisResult & result);

public:
    DeleteBitmapGetter delete_bitmap_getter;
    const size_t min_block_size;
    const bool size_predictor_estimate_lc_size_by_fullstate;
    const bool map_column_keys_column_queried;
    const bool sample_factor_column_queried;
};

}
