#pragma once

#include <memory>

#include <Common/CurrentThread.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ActionsVisitor.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/getTableExpressions.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/queryToString.h>
#include <Query/Processors/QueryPlan/AggregatingStepExt.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Query/Common/LinkedHashSet.h>
#include <Query/Executor/RuntimeFilter/RuntimeFilterUtils.h>
#include <Query/Parsers/ASTTableColumnReference.h>
#include <Query/Processors/QueryPlan/DistributedPipelineSettings.h>
#include <Query/Processors/QueryPlan/ExecutePlanElement.h>
#include <Query/Processors/QueryPlan/FilterStepExt.h>
#include <Query/Processors/QueryPlan/ProjectionStepExt.h>
#include <Query/Processors/QueryPlan/ReadFromMergeTreeExt.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/StorageReplicatedMergeTree.h>

namespace DB
{
using ConstASTPtr = std::shared_ptr<const IAST>;
using Assignment = std::pair<String, ConstASTPtr>;
using DataTypePtr = std::shared_ptr<const IDataType>;
using NameToType = std::map<String, DataTypePtr>;
using ASTSelectQueryPtr = std::shared_ptr<ASTSelectQuery>;
using RuntimeFilterId = UInt32;
struct BuildQueryPipelineSettingsExt;

StreamLocalLimits getLimitsForStorage(const Settings & settings, const SelectQueryOptions & options);

struct RewriteInQueryMatcher
{
    struct Data {
        std::vector<ASTPtr> ast_children_replacement;

        void replaceExpressionListChildren(const ASTFunction * fn);
    };

    static bool needChildVisit(ASTPtr & node, const ASTPtr & child);
    static void visit(ASTPtr & node, Data & data);
};
using RewriteInQueryVisitor = InDepthNodeVisitor<RewriteInQueryMatcher, true>;

class TableScanStepExt : public ISourceStep
{
public:
    // Server
    TableScanStepExt(
        ContextPtr context,
        StorageID storage_id_,
        const NamesWithAliases & column_alias_,
        const SelectQueryInfo & query_info_,
        size_t max_block_size_,
        String alias_ = "",
        bool bucket_scan_ = false,
        Assignments inline_expressions_ = {},
        std::shared_ptr<AggregatingStepExt> aggregation_ = nullptr,
        std::shared_ptr<ProjectionStepExt> projection_ = nullptr,
        std::shared_ptr<FilterStepExt> filter_ = nullptr);

    // Worker
    TableScanStepExt(
        ContextPtr context,
        DataStream output_stream_,
        StorageID storage_id_,
        NamesWithAliases column_alias_,
        SelectQueryInfo query_info_,
        size_t max_block_size_,
        String alias_,
        Assignments inline_expressions_,
        std::shared_ptr<AggregatingStepExt> aggregation_,
        std::shared_ptr<ProjectionStepExt> projection_,
        std::shared_ptr<FilterStepExt> filter_,
        DataStream table_output_stream_);

    // Copy
    TableScanStepExt(
        DataStream output,
        StoragePtr storage_,
        StorageID storage_id_,
        StorageMetadataPtr metadata_snapshot_,
        StorageSnapshotPtr storage_snapshot_,
        String original_table_,
        Names column_names_,
        NamesWithAliases column_alias_,
        SelectQueryInfo query_info_,
        size_t max_block_size_,
        String alias_,
        bool bucket_scan_,
        Assignments inline_expressions_,
        std::shared_ptr<AggregatingStepExt> aggregation_,
        std::shared_ptr<ProjectionStepExt> projection_,
        std::shared_ptr<FilterStepExt> filter_,
        DataStream table_output_stream_)
        : ISourceStep(std::move(output))
        , storage(storage_)
        , storage_id(storage_id_)
        , metadata_snapshot(metadata_snapshot_)
        , storage_snapshot(storage_snapshot_)
        , original_table(std::move(original_table_))
        , column_names(std::move(column_names_))
        , column_alias(std::move(column_alias_))
        , query_info(std::move(query_info_))
        , max_block_size(max_block_size_)
        , inline_expressions(std::move(inline_expressions_))
        , pushdown_aggregation(std::move(aggregation_))
        , pushdown_projection(std::move(projection_))
        , pushdown_filter(std::move(filter_))
        , table_output_stream(std::move(table_output_stream_))
        , bucket_scan(bucket_scan_)
        , alias(alias_)
        , log(getLogger("TableScanStepExt"))
    {
        if (storage)
            storage_id.uuid = storage->getStorageID().uuid;
    }

    String getName() const override { return "TableScanStepExt"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings  &) override;
    const String & getDatabase() const { return storage_id.database_name; }
    const String & getTable() const { return storage_id.table_name; }
    const String & getTableAlias() const { return alias; }
    void setOriginalTable(const String & original_table_) { original_table = original_table_; }
    const String & getOriginalTable() const { return original_table.empty() ? storage_id.table_name : original_table; }
    const Names & getColumnNames() const { return column_names; }
    const NamesWithAliases & getColumnAlias() const { return column_alias; }
    NameToNameMap getColumnToAliasMap() const;
    NameToNameMap getAliasToColumnMap() const;
    QueryProcessingStage::Enum getProcessedStage() const;
    size_t getMaxBlockSize() const;

    void setPushdownAggregation(QueryPlanStepPtr aggregation_)
    {
        pushdown_aggregation = std::dynamic_pointer_cast<AggregatingStepExt>(aggregation_);
    }
    void setPushdownProjection(QueryPlanStepPtr projection_)
    {
        pushdown_projection = std::dynamic_pointer_cast<ProjectionStepExt>(projection_);
    }
    void setPushdownFilter(QueryPlanStepPtr filter_)
    {
        pushdown_filter = std::dynamic_pointer_cast<FilterStepExt>(filter_);
    }
    std::shared_ptr<AggregatingStepExt> getPushdownAggregation() const { return pushdown_aggregation; }
    std::shared_ptr<ProjectionStepExt> getPushdownProjection() const { return pushdown_projection; }
    std::shared_ptr<FilterStepExt> getPushdownFilter() const { return pushdown_filter; }
    const AggregatingStepExt * getPushdownAggregationCast() const { return dynamic_cast<AggregatingStepExt *>(pushdown_aggregation.get()); }
    const ProjectionStepExt * getPushdownProjectionCast() const { return dynamic_cast<ProjectionStepExt *>(pushdown_projection.get()); }
    const FilterStepExt * getPushdownFilterCast() const { return dynamic_cast<FilterStepExt *>(pushdown_filter.get()); }
    AggregatingStepExt * getPushdownAggregationCast() { return dynamic_cast<AggregatingStepExt *>(pushdown_aggregation.get()); }
    ProjectionStepExt * getPushdownProjectionCast() { return dynamic_cast<ProjectionStepExt *>(pushdown_projection.get()); }
    FilterStepExt * getPushdownFilterCast() { return dynamic_cast<FilterStepExt *>(pushdown_filter.get()); }

    void setInlineExpressions(Assignments new_inline_expressions, ContextPtr context);
    const Assignments & getInlineExpressions() const
    {
        return inline_expressions;
    }
    bool hasInlineExpressions() const
    {
        return !inline_expressions.empty();
    }

    const DataStream & getTableOutputStream() const
    {
        return table_output_stream;
    }

    void setReadOrder(SortDescription read_order);
    SortDescription getReadOrder() const;

    void formatOutputStream(ContextPtr context);

    bool setLimit(size_t limit, const ContextMutablePtr & context);
    bool hasLimit() const;
    bool hasPrewhere() const;
    ASTPtr getPrewhere() const;

    SelectQueryInfo fillQueryInfo(ContextPtr context);
    void fillPrewhereInfo(ContextPtr context);
    void makeSetsForIndex(const ASTPtr & node, ContextPtr context, PreparedSets & prepared_sets, const NamesAndTypesList & source) const;
    void fillQueryInfoV2(ContextPtr context);

    void allocate(ContextPtr context);
    Int32 getUniqueId() const { return unique_id; }
    void setUniqueId(Int32 unique_id_) { unique_id = unique_id_; }
    bool isBucketScan() const { return bucket_scan; }
    void setBucketScan(bool bucket_scan_) { bucket_scan = bucket_scan_; }
    // ues for plan cache
    void cleanStorage();
    void setStorage(ContextPtr context) { storage = DatabaseCatalog::instance().getTable(storage_id, context); }
    std::shared_ptr<IStorage> getStorage() const;
    const SelectQueryInfo & getQueryInfo() const { return query_info; }
    SelectQueryInfo & getQueryInfo()
    {
        return query_info;
    }
    const StorageID & getStorageID() const { return storage_id; }
    StorageMetadataPtr getMetadataSnapshot() const { return metadata_snapshot; }
    StorageSnapshotPtr getStorageSnapshot() const { return storage_snapshot; }
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr context) const;

    enum GetFlags : UInt32
    {
        Output = 1,
        Prewhere = 2,
        BitmapIndex = 4,

        OutputAndPrewhere = Output | Prewhere,
        All = Output | Prewhere | BitmapIndex,
    };

    Names getRequiredColumns(GetFlags flags = All) const;
    void rewriteInForBucketTable(ContextPtr context) const;
    void setQuotaAndLimits(QueryPipelineBuilder & pipeline, const SelectQueryOptions & options, const BuildQueryPipelineSettings  & build_context);

    void toProto(Protos::TableScanStepExt & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<TableScanStepExt> fromProto(const Protos::TableScanStepExt & proto, ContextPtr context);

private:
    StoragePtr storage;
    StorageID storage_id;
    StorageMetadataPtr metadata_snapshot;
    StorageSnapshotPtr storage_snapshot;
    String original_table;
    Names column_names; // TODO: remove me, use column_alias instead
    NamesWithAliases column_alias;
    // Used for passing some important information to instruct data reading processing, including
    // - query.where(), condition of this storage, used for index pruning
    // - query.prewhere(), user specified PREWHERE of this storage. TODO: move move_where_to_prewhere optimize into PlanOptimizer
    // - partition_filter, condition on partition keys, used for partition pruning
    // - query.limit(), result limit of table scan
    SelectQueryInfo query_info;
    size_t max_block_size;

    // Expressions which can be calculated by IStorage::read/readFromParts, including
    // - bitmap index expressions
    // - sub expression of prewhere
    Assignments inline_expressions;

    // Pushdown steps. Now TableScanStep is not like a single step anymore, but more like a sub plan
    // with structure `Partial Aggregate->Projection->Filter->ReadTable`. And we are able to use
    // **clickhouse projection** to optimize its execution.
    // TODO: better to use a new kind of IQueryPlanStep
    std::shared_ptr<AggregatingStepExt> pushdown_aggregation;
    std::shared_ptr<ProjectionStepExt> pushdown_projection;
    std::shared_ptr<FilterStepExt> pushdown_filter;
    DataStream table_output_stream;

    // just for cascades, in order to distinguish between the same tables.
    Int32 unique_id{0};
    bool bucket_scan;
    String alias;

    // Only for worker.
    bool is_null_source{false};

    LoggerPtr log;

    void rewriteDynamicFilter(SelectQueryInfo & select_query, const BuildQueryPipelineSettings  & build_settings, bool use_expand_pipe);

    void aliasColumns(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings  &, const String & pipeline_name);

    bool hasFunctionCanUseBitmapIndex() const;
    void initMetadataAndStorageSnapshot(ContextPtr context);
    Names getRequiredColumnsAndPartitionColumns() const;
};

}
