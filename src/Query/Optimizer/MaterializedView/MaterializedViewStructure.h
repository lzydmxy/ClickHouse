#pragma once

#include <unordered_map>
#include <Query/Analyzer/ASTEquals.h>
#include <Query/Optimizer/EqualityASTMap.h>
#include <Query/Optimizer/SymbolTransformMap.h>
#include <Query/Optimizer/Utils.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/StorageMaterializedView.h>
#include <Query/Optimizer/MaterializedView/MaterializedViewJoinHyperGraph.h>

namespace DB
{
struct MaterializedViewStructure;
using MaterializedViewStructurePtr = std::shared_ptr<MaterializedViewStructure>;

using ExpressionEquivalences = Equivalences<ConstASTPtr, EqualityASTMap>;

/**
 * Structural information extracted from materialized view, used for materialized view rewriting.
 */
struct MaterializedViewStructure
{
    /**
     * @param view_storage_id name of materialized view
     * @param target_storage_id name of materialized view storage table
     * @param query materialized view query ast
     * @return structure info if it support materialized view rewriting, empty otherwise.
     */
    static MaterializedViewStructurePtr
    buildFrom(const StorageID & view_storage_id, const StorageID & target_storage_id, ASTPtr query, bool async_materialized_view, ContextPtr context);

    /**
     * @param view_storage_id name of materialized view
     * @param target_storage_id name of materialized view storage table
     * @param query create table query statement after optimizer RBO ruls of materialized view
     * @return structure info if it support materialized view rewriting, empty otherwise.
     */
    static MaterializedViewStructurePtr
    buildFrom(const StorageID & view_storage_id, const StorageID & target_storage_id, PlanNodePtr query, bool async_materialized_view, ContextPtr context);

    const StorageID view_storage_id;
    const StorageID target_storage_id;
    const StoragePtr target_storage;
    const std::unordered_set<StorageID> base_storage_ids;

    const JoinHyperGraph join_hyper_graph;
    const PlanNodes inner_sources;
    const PlanNodes outer_sources;

    const std::shared_ptr<const AggregatingStepExt> top_aggregating_step;
    const bool having_predicates;

    const SymbolTransformMap symbol_map;
    const std::unordered_set<String> output_columns;
    const std::unordered_map<String, String> output_columns_to_table_columns_map;
    const std::unordered_map<String, String> output_columns_to_query_columns_map;
    const ExpressionEquivalences expression_equivalences;

    bool async_materialized_view;

    MaterializedViewStructure(
        StorageID view_storage_id_,
        StorageID target_storage_id_,
        StoragePtr target_storage_,
        std::unordered_set<StorageID> base_storage_ids_,
        JoinHyperGraph join_hyper_graph_,
        PlanNodes inner_sources_,
        PlanNodes outer_sources_,
        std::shared_ptr<const AggregatingStepExt> top_aggregating_step_,
        bool having_predicates_,
        SymbolTransformMap symbol_map_,
        std::unordered_set<String> output_columns_,
        std::unordered_map<String, String> output_columns_to_table_columns_map_,
        std::unordered_map<String, String> output_columns_to_query_columns_map_,
        ExpressionEquivalences expression_equivalences_,
        bool async_materialized_view_)
        : view_storage_id(std::move(view_storage_id_))
        , target_storage_id(std::move(target_storage_id_))
        , target_storage(std::move(target_storage_))
        , base_storage_ids(std::move(base_storage_ids_))
        , join_hyper_graph(std::move(join_hyper_graph_))
        , inner_sources(std::move(inner_sources_))
        , outer_sources(std::move(outer_sources_))
        , top_aggregating_step(std::move(top_aggregating_step_))
        , having_predicates(std::move(having_predicates_))
        , symbol_map(std::move(symbol_map_))
        , output_columns(std::move(output_columns_))
        , output_columns_to_table_columns_map(std::move(output_columns_to_table_columns_map_))
        , output_columns_to_query_columns_map(std::move(output_columns_to_query_columns_map_))
        , expression_equivalences(std::move(expression_equivalences_))
        , async_materialized_view(async_materialized_view_)
    {
    }
};
}
