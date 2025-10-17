#pragma once

#include <base/types.h>
#include <Core/Defines.h>
#include <Core/BaseSettings.h>
#include <Core/SettingsEnums.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/JSON/Object.h>
#include <Common/NamePrompter.h>

namespace DB
{

enum class QueryDryRunMode
{
    NONE,
    SKIP_SEND_PARTS,
    SKIP_READ_PARTS,
    SKIP_EXECUTE_SEGMENT,
    SKIP_EXECUTE_QUERY,
};

DECLARE_SETTING_ENUM(QueryDryRunMode);

/// Scheduler sort shards randomly, in order, and rank on resource utilization
enum class SchedulerMode
{
    RANDOM = 0,
    FIRST_ORDER,
    RANDOM_ORDER,
    CPU_RANK,
    MEMORY_RANK,
};
DECLARE_SETTING_ENUM(SchedulerMode);

enum class CTEMode
{
    INLINED,
    SHARED,
    AUTO,
    ENFORCED,
};

enum class MaterializedViewConsistencyCheckMethod
{
    NONE,
    PARTITION,
};

DECLARE_SETTING_ENUM(MaterializedViewConsistencyCheckMethod);

enum class DialectType {
    CLICKHOUSE,
    ANSI,
    MYSQL,
};

DECLARE_SETTING_ENUM(CTEMode)
DECLARE_SETTING_ENUM(DialectType)

enum class SpillMode
{
    MANUAL,
    AUTO,
};

DECLARE_SETTING_ENUM(SpillMode)

enum class StatisticsAccurateSampleNdvMode
{
    NEVER,
    AUTO,
    ALWAYS,
};
DECLARE_SETTING_ENUM(StatisticsAccurateSampleNdvMode)

enum class StatisticsCachePolicy
{
    Default,
    Cache,
    Catalog,
};
DECLARE_SETTING_ENUM(StatisticsCachePolicy)

constexpr UInt64 RUNTIME_FILTER_BLOOM_BUILD_THRESHOLD = 2048000; // Default threshold of right table to build bloom filter
constexpr UInt64 RUNTIME_FILTER_IN_BUILD_THRESHOLD = 1024; // Default threshold of right table to build value set filter
// Statistics
constexpr uint64_t DEFAULT_KLL_SKETCH_LOG_K = 1600;

/** These settings represent fine tunes for internal details of query optimizer
 * and should not be changed by the user without a reason.
  */
#define LIST_OF_OPTIMIZER_SETTINGS(M, ALIAS) \
    /** Query optimizer relative settings*/ \
    M(Bool, enable_legacy_optimizer, false, "Whether enable query optimizer", 0) \
    M(Bool, enable_optimizer_fallback, false, "Whether enable query optimizer fallback to clickhouse origin when failed", 0) \
    M(UInt64, exchange_buffer_send_threshold_in_bytes, 1000000, "The minimum bytes when exchange will flush send buffer ", 0) \
    M(UInt64, exchange_buffer_send_threshold_in_row, 65505, "The minimum row num when exchange will flush send buffer", 0) \
    M(Bool, exchange_enable_force_remote_mode, false, "Force exchange data transfer through network", 0) \
    M(Bool, exchange_force_use_buffer, false, "Force exchange use buffer as possible", 0) \
    M(Bool, enable_prune_source_plan_segment, false, "Whether prune source plan segment", 0) \
    M(UInt64, interactive_delay_optimizer_mode, 0, "The interval(in optimizer mode) in microseconds to check if the request is cancelled, and to send progress info.", 0) \
    M(Bool, adaptive_type_cast, true, "Performs type cast operations adaptively, according to the value", 0) \
    M(Bool, parse_literal_as_decimal, false, "Parse numeric literal as decimal instead of float", 0) \
    M(Int64, final_order_by_all_direction, 0, "Sorting the most 'end' result for select query, default 0 means no sorting, > 1 for ASC, < -1 for DESC", 0) \
    M(Bool, enable_memory_catalog, true, "Enable memory catalog for unittest", 0) \
    M(Bool, print_graphviz, false, "Whether print graphviz", 0) \
    M(String, graphviz_path, "/tmp/plan/", "The path of graphviz plan", 0) \
    M(Bool, print_graphviz_ast, false, "Whether print graphviz", 0) \
    M(Bool, print_graphviz_planner, true, "Whether print graphviz", 0) \
    M(UInt64, plan_optimizer_rule_warning_time, 1000, "Send warning if a optimize rule optimize time exceed timeout", 0) \
    M(Bool, group_by_two_level_for_grouping_set, true, "Adaptive two-level aggregation is not valid for grouping set queries. Setting 1 to enforce two-level aggregation, 0 to enforce single-level aggregation.", 0) \
    M(UInt64, memory_catalog_worker_size, 8, "Memory catalog work size for unittest", 0) \
    M(UInt64, plan_optimizer_timeout, 600000, "Max running time of a plan rewriter optimizer in ms", 0) \
    M(Bool, enable_active_prewhere, false, "Whether to actively generate prewhere by statistics", 0) \
    M(Float, max_active_prewhere_selectivity, 0.3f, "Max Selectivity of actively generated prewheres", 0) \
    M(UInt64, max_active_prewhere_size, 3, "Max Size of to actively generated prewheres", 0) \
    M(Bool, log_normalized_query_plan_hash, 0, "Log json format query plan to the system query_log table.", 0) \
    M(Bool, log_query_exchange, false, "Log query exchange metric.", 0) \
    M(Bool, log_plan_after_each_rewriter, false, "log query plan after each rewriter rewrite plan", 0) \
    /** Coordinator settings*/ \
    M(SchedulerMode, scheduler_mode, SchedulerMode::RANDOM, "scheduler shard mode: random/first_order/random_order/cpu_rank/memory_rank", 0) \
    M(UInt64, operator_profile_receive_timeout, 3000, "Max waiting time for operator profile in ms", 0) \
    M(UInt64, push_queue_timeout_millseconds, 10, "Timeout millseconds of push profile or others to queue.", 0) \
    /** Exchange settings */ \
    M(UInt64, exchange_timeout_ms, 300000, "Exchange request timeout ms",0) \
    M(UInt64, exchange_queue_bytes, 209715200, "Queue size(bytes) for exchange queue, 0 means disable", 0) \
    M(Bool, exchange_use_query_memory_tracker, true, "Use query-level memory tracker", 0) \
    M(UInt64, exchange_parallel_size, 1, "Exchange parallel size", 0) \
    M(UInt64, distributed_query_wait_exception_ms, 2000,"Wait final planSegment exception from segmentScheduler.", 0) \
    M(Bool, enable_wait_for_post_processing, false, "Whether a query needs to wait for post processing rpcs done before end", 0) \
    M(UInt64, wait_for_post_processing_timeout_ms, 1000, "Timeout for waiting post processing rpc from workers.", 0) \
    M(UInt64, exchange_wait_accept_max_timeout_ms, 20000, "Exchange receiver wait accept max timeout ms",0) \
    M(UInt64, exchange_unordered_output_parallel_size, 4, \
        "The num of exchange sink for unorder exchange, ingoned if exchange need keep data order ", 0) \
    M(Bool, exchange_enable_force_keep_order, false, "Force exchange keep data order", 0) \
    M(Bool, exchange_enable_keep_order_parallel_shuffle, false, "Whether enable parallel shuffle when exchange need keep order", 0) \
    M(UInt64, disk_shuffle_advisory_partition_size, 104857600, "Disk shuffle files's advisory partition size(including all files in a partition), used by partition coalescing", 0) \
    M(Bool, enable_disk_shuffle_partition_coalescing, false, "If enabled, sheduler will try to coalesce overly-small partitions, thus avoid small plan segments and I/O waste", 0) \
    M(Bool, enable_batch_send_plan_segment, true, "Whether enable combined sending plan segments to reduce rpc calls", 0) \
    M(UInt64, exchange_remote_receiver_queue_size, 10, "Queue size for remote exchange receiver",0) \
    M(UInt64, exchange_stream_max_buf_size, 20971520, "Default 20M, -1 means no limit", 0) \
    M(Bool, exchange_enable_block_compress, true, "Whether enable exchange block compress ", 0) \
    M(UInt64, exchange_local_receiver_queue_size, 30, "Queue size for local exchange receiver",0) \
    M(UInt64, exchange_multi_path_receiver_queue_size, 20, "Queue size for multi path exchange receiver", 0) \
    M(UInt64, exchange_stream_back_pressure_max_wait_ms, 0, "Default 0, 0 means no control", 0) \
    M(Bool, exchange_enable_multipath_receiver, true, "Whether enable exchange new mode ", 0) \
    M(Float, exchange_source_pipeline_threads_to_max_threads_ratio, 1, "Recommend number of threads for pipeline which reading data from exchange, ingoned if exchange need keep data order ", 0) \
    /** Runtime Filter settings */ \
    M(String, runtime_filter_black_list, "", "Runtime filter ids need be blocked", 0) \
    M(UInt64, runtime_filter_min_filter_rows, 10000, "Set minimum row to enable runtime filter", 0) \
    M(Float, runtime_filter_min_filter_factor, 0.4f, "Set minimum filter factor to enable runtime filter", 0) \
    M(Float, runtime_filter_min_filter_factor_for_non_table_scan, 0.9f, "Set minimum filter factor to enable runtime filter if runtime filter can not pushdown", 0) \
    M(Bool, enable_runtime_filter, false, "Whether enable runtime filter for join", 0) \
    M(Bool, enable_runtime_filter_pipeline_poll, true, "No additional segment needed for the left side during broadcast join, polling time bounded", 0) \
    M(UInt64, wait_runtime_filter_timeout, 1000, "Execute filter wait for runtime filter timeout ms", 0) \
    M(Bool, enable_range_cover, true, "Whether use range rather than bloom or values set for runtime filter", 0) \
    M(UInt64, clean_rf_time_limit, 300000, "Threshold to clean runtime filters in manager to prevent memory leak", 0) \
    M(Bool, enable_rewrite_bf_into_prewhere, true, "Whether enable pushdown runtime filter to prewhere for join", 0) \
    M(UInt64, runtime_filter_bloom_build_threshold, RUNTIME_FILTER_BLOOM_BUILD_THRESHOLD, "The threshold of right table to build bloom filter", 0) \
    M(UInt64, runtime_filter_in_build_threshold, RUNTIME_FILTER_IN_BUILD_THRESHOLD, "The threshold of right table to build value set filter", 0) \
    M(Double, adjust_range_set_filter_rate, 0.10, "If the prewhere is not range or set, adjust use this value as priority to bloom filter ", 0) \
    /** Settings for intermediate result cache */ \
    M(Bool, enable_intermediate_result_cache, false, "Whether to enable intermediate result cache.", 0) \
    M(Bool, enable_join_intermediate_result_cache, false, "Whether to enable join intermediate result cache.", 0) \
    M(Bool, enable_intermediate_result_cache_ignore_partition_filter, true, "Whether to ignore parition filter in intermediate result cache.", 0) \
    M(Bool, enable_intermediate_result_cache_streaming, false, "Whether to enable streaming agg for intermediate result cache.", 0) \
    M(Seconds, wait_intermediate_result_cache, 60, "Time to wait for enable intermediate result cache per part, 0 means disable.", 0) \
    M(UInt64, intermediate_result_cache_max_bytes, 100000000, "Intermediate result cache entry max bytes, 0 means disable.", 0) \
    M(UInt64, intermediate_result_cache_max_rows, 100000, "Intermediate result cache entry max rows, 0 means disable.", 0) \
    \
    /** Optimizer join settings */ \
    M(Bool, enforce_all_join_to_any_join, false, "Whether enforce all join to any join", 0) \
    M(Bool, enable_nested_loop_join, false, "Whether enable nest loop join for outer join with filter", 0)\
    M(Bool, use_grace_hash_only_repartition, false, "Only use grace hash join when exchange type is repartition", 0) \
    M(UInt64, grace_hash_join_left_side_parallel, 1, "Initial number of grace hash join left side parallel", 0) \
    M(Bool, join_using_null_safe, 0, "Force null safe equal comparison for USING keys except the last key of ASOF join", 0) \
    /** Debug settings */ \
    M(Bool, disable_single_server_optimization, false, "Whether disable optimization for single server cluster, you can disable it when debugging distributed query plan in a single server cluster", 0) \
    M(Bool, log_segment_profiles, false, "Log profile of each segment info including runtime and planning information.", 0) \
    M(Bool, report_segment_profiles, false, "Report plan segment profile to coordinator.", 0) \
    /** */ \
    M(Bool, convert_to_right_type_for_in_subquery, true, "For IN subquery, whether convert arguments to the right type", 0) \
    /** Optimizer relative settings, Plan build and RBO */ \
    M(Bool, enable_auto_prepared_statement, false, "Whether to enable automatic prepared statement", 0) \
    M(Bool, enable_implicit_type_conversion, true, "Whether enable implicit type conversion for JOIN, Set operation, IN subquery", 0) \
    M(Bool, rewrite_like_function, true, "Rewrite simple pattern like function", 0) \
    M(UInt64, iterative_optimizer_timeout, 10000, "Max running time of a single iterative optimizer in ms", 0) \
    M(Bool, debug_iterative_optimizer, false, "If enabled, iterative optimizer will print plan after each rule application", 0) \
    M(Bool, enable_remove_uncorrelated_in_subquery, true, "Whether enable remove uncorrelated in subquery", 0) \
    M(Bool, enable_remove_correlated_in_subquery, true, "Whether enable remove correlated in subquery", 0) \
    M(Bool, enable_remove_uncorrelated_exists_subquery, true, "Whether enable remove uncorrelated exists subquery", 0) \
    M(Bool, enable_remove_correlated_exists_subquery, true, "Whether enable remove correlated exists subquery", 0) \
    M(Bool, enable_remove_uncorrelated_scalar_subquery, true, "Whether enable remove uncorrelated scalar subquery", 0) \
    M(Bool, enable_remove_correlated_scalar_subquery, true, "Whether enable remove correlated scalar subquery", 0) \
    M(Bool, enable_remove_uncorrelated_quantified_comparison_subquery, true, "Whether enable remove correlated quantified comparison subquery", 0) \
    M(Bool, enable_remove_correlated_quantified_comparison_subquery, true, "Whether enable remove correlated quantified comparison subquery", 0) \
    M(Bool, enable_unnesting_subquery_with_window, true, "Whether enable unnesting subquery with window", 0) \
    M(Bool, enable_unnesting_subquery_with_semi_anti_join, true, "Whether enable unnesting subquery with semi anti join", 0) \
    M(Bool, eliminate_cross_joins, true, "Whether eliminate cross joins", 0) \
    M(Bool, enable_unify_join_outputs, true, "Whether enable unify join output ", 0) \
    M(Bool, enable_unify_nullable_type, true, "Whether enable unify nullable type", 0) \
    M(Bool, enable_sorting_property, true, "Whether enable sorting property rule", 0) \
    M(Bool, enable_streaming_property, true, "Whether enable streaming property rule", 0) \
    M(Bool, enable_use_node_property, true, "Whether enable node property rule", 0) \
    M(Bool, enable_distinct_to_aggregate, true, "Whether enable convert distinct to group by", 0) \
    M(Bool, enable_cross_join_to_union, false, "Whether enable convert cross join to union", 0) \
    M(Bool, enable_distinct_remove, true, "Whether to eliminate redundancy during execution", 0) \
    M(Bool, enable_single_distinct_to_group_by, true, "Whether enable convert single count distinct to group by", 0) \
    M(Bool, enable_mark_distinct_optimzation, false, "Whether enable Mark distinct optimization", 0)                                            \
    M(Bool, enable_expand_distinct_optimization, false, "Whether enable rewrite distinct optimization", 0)                                            \
    M(ExpandMode, expand_mode, ExpandMode::EXPAND, "Rewrite distinct optimization, Expand Mode : EXPAND|UNION|CTE", 0)                                            \
    M(Bool, enable_common_predicate_rewrite, true, "Whether enable common predicate rewrite", 0) \
    M(Bool, enable_common_join_predicate_rewrite, true, "Whether enable common predicate rewrite", 0) \
    M(Bool, enable_swap_predicate_rewrite, true, "Whether enable swap predicate rewrite", 0) \
    M(Bool, rewrite_predicate_by_domain, true, "When enabled, merge predicates belonging to the same domain", 0) \
    M(Bool, rewrite_complex_predicate_by_domain, false, "Whether enabled, extract merged predicate belonging to the same domain for complex predicate(which normally are DNFs)", 0) \
    M(Bool, enable_unwrap_cast_in, true, "Whether enable unwrap cast function", 0) \
    M(Bool, enable_windows_reorder, true, "Reorder adjacent windows to decrease exchange", 0) \
    M(Bool, enable_push_partial_agg, true, "Whether enable push partial agg", 0) \
    M(Bool, enable_remove_final_agg, true, "Whether enable remove final agg", 0) \
    M(Bool, enable_cbo_push_partial_agg, false, "Whether enable cost base push partial agg", 0) \
    M(Bool, enable_shuffle_before_state_func, true, "Whether shuffle when agg func is state func.", 0) \
    M(Bool, enable_share_common_plan_node, true, "Whether enable share common plan node using cte", 0) \
    M(Bool, enable_redundant_sort_removal, true, "Whether enable ignore redundant sort in subquery", 0) \
    M(Bool, enable_remove_unused_cte, true, "Whether enable remove unused cte", 0) \
    M(Bool, enable_filter_window_to_partition_topn, true, "Filter window to partition topn", 0) \
    M(Bool, enable_partition_filter_push_down, true, "Allow to push down partition filter to query info", 0) \
    M(Bool, external_enable_partition_filter_push_down, true, "Allow to push down partition filter to query info for external table. Consider to merge into enable_partition_filter_push_down when mergetree bug is fixed", 0) \
    M(Bool, enable_optimizer_early_prewhere_push_down, false, "Allow to push down prewhere in the optimizer phase", 0) \
    M(Bool, enable_optimizer_support_window, true, "Optimizer support window", 0) \
    M(Bool, enable_filter_window_to_sorting_limit, true, "Filter window to sorting limit", 0) \
    M(Bool, optimizer_projection_support, false, "Use projection in optimizer mode", 0) \
    M(Bool, optimizer_index_projection_support, true, "Use indexprojection in optimizer mode", 0) \
    M(Bool, enable_setoperation_to_agg, true, "Whether enable rewrite set operation to aggregation", 0)                                            \
    M(Bool, enable_execute_uncorrelated_subquery, false, "Whether enable execute uncorrelated subquery", 0) \
    M(UInt64, execute_uncorrelated_in_subquery_size, 10000, "Size of execute uncorrelated in subquery", 0) \
    M(Bool, enable_subcolumn_optimization_through_union, true, "Whether enable sub column optimization through set operation.", 0) \
    M(Bool, enable_buffer_for_deadlock_cte, true, "Whether to buffer data for deadlock cte", 0) \
    M(UInt64, statistics_collect_debug_level, 0, "Debug level for statistics collector", 0) \
    M(Bool, enable_remove_remove_unnecessary_buffer, false, "Whether to only add buffer for cte consumer that may cause deadlock", 0) \
    M(Int64, max_buffer_size_for_deadlock_cte, 13000000000, "Inline CTE if buffer is oversized, set 0 to inline all cte, set -1 to buffer data for all cte even no stats", 0) \
    M(UInt64, max_prewhere_or_expression_size, 0, "Max depth of condition which can push down to prewhere", 0) \
    M(Bool, enable_add_exchange, true, "Whether to enable AddExchange rule", 0) \
    M(Bool, enable_bitmap_index_splitter, true, "Whether to enable BitMapIndexSplitter", 0) \
    M(Bool, enable_column_pruning, true, "Whether to enable ColumnPruning", 0) \
    M(Bool, enable_add_projection_to_pruning, true, "Whether add projection when column pruning", 0) \
    M(Bool, enable_predicate_pushdown_rewrite, true, "Whether to enable PredicatePushdown", 0) \
    M(Bool, enable_pushdown_filter_through_stateful, false, "Whether to enable push predicate through projection with stateful functions", 0) \
    M(Bool, enable_hints_propagator, true, "Whether to enable HintsPropagator", 0) \
    M(Bool, enable_join_algorithm_hints, true, "Whether to enable ImplementJoinAlgorithmHints", 0) \
    M(Bool, enable_join_operation_hints, true, "Whether to enable ImplementJoinOperationHints", 0) \
    M(Bool, enable_join_order_hint, true, "Whether to enable ImplementJoinOrderHints", 0) \
    M(Bool, enable_set_join_distribution, true, "Whether to enable SetJoinDistribution rule", 0) \
    M(Bool, enable_explain_analyze, true, "Whether to enable ExplainAnalyze rule", 0) \
    M(Bool, enable_implement_except, true, "Whether to enable ImplementExceptRule rule", 0) \
    M(Bool, enable_implement_intersect, true, "Whether to enable ImplementIntersectRule rule", 0) \
    M(Bool, enable_inline_projection, true, "Whether to enable InlineProjections rule", 0) \
    M(Bool, enable_inline_projection_into_join, true, "Whether to enable InlineProjections rule", 0) \
    M(Bool, enable_inline_projection_on_join_into_join, true, "Whether to enable InlineProjections rule", 0) \
    M(Bool, enable_merge_aggregate, true, "Whether to enable MergeAggregatings rule", 0) \
    M(Bool, enable_merge_union, true, "Whether to enable MergeUnionRule rule", 0) \
    M(Bool, enable_merge_except, true, "Whether to enable MergeExceptRule rule", 0) \
    M(Bool, enable_merge_intersect, true, "Whether to enable MergeIntersectRule rule", 0) \
    M(Bool, enable_pull_projection_on_join_through_join, true, "Whether to enable PullProjectionOnJoinThroughJoin rule", 0) \
    M(Bool, enable_push_agg_through_outer_join, true, "Whether to enable PushAggThroughOuterJoin rule", 0) \
    M(Bool, enable_push_agg_through_inner_join, true, "Whether to enable PushAggThroughInnerJoin rule", 0) \
    M(Bool, enable_push_limit_into_distinct, true, "Whether to enable PushLimitIntoDistinct rule", 0) \
    M(Bool, enable_push_limit_through_projetion, true, "Whether to enable PushLimitThroughProjection rule", 0) \
    M(Bool, enable_push_limit_through_extremes, true, "Whether to enable PushLimitThroughExtremes rule", 0) \
    M(Bool, enable_push_limit_through_union, true, "Whether to enable PushLimitThroughUnion rule", 0) \
    M(Bool, enable_push_limit_through_outer_join, true, "Whether to enable PushLimitThroughOuterJoin rule", 0) \
    M(Bool, enable_limit_zero_to_read_nothing, true, "Whether to enable LimitZeroToReadNothing rule", 0) \
    M(Bool, enable_push_down_limit_into_window, true, "Whether to enable PushdownLimitIntoWindow rule", 0) \
    M(Bool, enable_push_limit_into_sorting_rule, true, "Whether to enable PushLimitIntoSorting rule", 0) \
    M(Bool, enable_push_limit_through_buffer, true, "Whether to enable PushLimitThroughBuffer rule", 0) \
    M(Bool, enable_push_down_apply_through_join, true, "Whether to enable PushDownApplyThroughJoin rule", 0) \
    M(Bool, enable_push_storage_filter, true, "Whether to enable PushStorageFilter rule", 0) \
    M(Bool, enable_push_limit_into_table_scan, true, "Whether to enable PushLimitIntoTableScan rule", 0) \
    M(Bool, enable_push_aggregation_into_table_scan, true, "Whether to enable PushAggregationIntoTableScan rule", 0) \
    M(Bool, enable_push_projection_into_table_scan, true, "Whether to enable PushProjectionIntoTableScan rule", 0) \
    M(Bool, enable_push_index_projection_into_table_scan, true, "Whether to enable PushIndexProjectionIntoTableScan rule", 0) \
    M(Bool, enable_push_filter_into_table_scan, true, "Whether to enable PushFilterIntoTableScan rule", 0) \
    M(Bool, enable_push_union_through_join, true, "Whether to enable PushUnionThroughJoin rule", 0) \
    M(Bool, enable_push_union_through_projection, true, "Whether to enable PushUnionThroughProjection rule", 0) \
    M(Bool, enable_push_union_through_agg, false, "Whether to enable PushUnionThroughAgg rule", 0) \
    M(Bool, enable_inner_join_associate, true, "Whether to enable InnerJoinAssociate rule", 0) \
    M(Bool, enable_inner_join_commutation, true, "Whether to enable InnerJoinCommutation rule", 0) \
    M(Bool, enable_join_enum_on_graph, true, "Whether to enable JoinEnumOnGraph rule", 0) \
    M(Bool, enable_join_to_multi_join, true, "Whether to enable JoinToMultiJoin rule", 0) \
    M(Bool, enable_cardinality_based_join_reorder, true, "Whether to enable CardinalityBasedJoinReorder rule", 0) \
    M(Bool, enable_selectivity_based_join_reorder, true, "Whether to enable SelectivityBasedJoinReorder rule", 0) \
    M(Bool, enable_left_join_to_right_join, true, "Whether to enable LeftJoinToRightJoin rule", 0) \
    M(Bool, enable_pull_outer_join, true, "Whether to enable PullOuterJoin rule", 0) \
    M(Bool, enable_push_join_through_union, true, "Whether to enable PushJoinThroughUnion rule", 0) \
    M(Bool, enable_semi_join_push_down, true, "Whether to enable SemiJoinPushDown rule", 0) \
    M(Bool, enable_simplify_predicate_rewrite, true, "Whether to enable SimplifyPredicateRewrite rule", 0) \
    M(Bool, enable_simplify_expression_by_derived_constant, false, "Whether to use derived constants to simplify expression", 0) \
    M(Bool, enable_simplify_prewhere_rewrite, true, "Whether to enable SimplifyPrewhereRewrite rule", 0) \
    M(Bool, enable_simplify_join_filter_rewrite, true, "Whether to enable SimplifyJoinFilterRewrite rule", 0) \
    M(Bool, enable_simplify_expression_rewrite, true, "Whether to enable SimplifyExpressionRewrite rule", 0) \
    M(Bool, enable_simplify_predicate_in_projection, false, "Whether to rewrite predicate in projection", 0) \
    M(Bool, enable_simplify_assume_not_null, true, "Whether to remove redundant assumeNotNull --temporary settings", 0) \
    M(Bool, enable_evaluate_constant_for_nondeterministic, true, "Enable evaluate the constant result of non-deterministic functions while using optimizer", 0) \
    M(Bool, enable_remove_redundant, true, "Whether to enable RemoveRedundant rules", 0) \
    M(Bool, enable_push_projection, true, "Whether to enable PushProjection rules", 0) \
    M(Bool, enable_push_partial_agg_through_exchange, true, "Whether to enable PushPartialAggThroughExchange rules", 0) \
    M(Bool, enable_push_partial_agg_through_union, true, "Whether to enable PushPartialAggThroughUnion rules", 0) \
    M(Bool, enable_push_partial_sorting_through_exchange, true, "Whether to enable PushPartialSortingThroughExchange rules", 0) \
    M(Bool, enable_push_partial_sorting_through_union, true, "Whether to enable PushPartialSortingThroughUnion rules", 0) \
    M(Bool, enable_push_partial_limit_through_exchange, true, "Whether to enable PushPartialLimitThroughExchange rules", 0) \
    M(Bool, enable_push_partial_distinct_through_exchange, true, "Whether to enable PushPartialDistinctThroughExchange rules", 0) \
    M(Bool, enable_push_partial_topn_distinct_through_exchange, true, "Whether to enable PushPartialTopNDistinctThroughExchange rules", 0) \
    M(Bool, enable_push_projection_through_exchange, true, "Whether to enable PushProjectionThroughExchange rules", 0) \
    M(Bool, enable_split_countd_to_state_merge, false, "Whether to enable split count distinct to state and merge", 0) \
    M(UInt64, max_rows_to_use_topn_filtering, 0, "The maximum N of TopN to use topn filtering optimization. Set 0 to choose this value adaptively.", 0) \
    M(String, topn_filtering_algorithm_for_unsorted_stream, "SortAndLimit", "The default topn filtering algorithm for unsorted stream, can be one of: 'SortAndLimit', 'Heap'", 0) \
    M(Bool, enable_create_topn_filtering_for_aggregating, false, "Whether to enable CreateTopNFilteringForAggregating rules", 0) \
    M(Bool, enable_push_sort_through_projection, true, "Whether to enable PushTopNThroughProjection rules", 0) \
    M(Bool, enable_push_topn_through_projection, false, "Whether to enable PushTopNThroughProjection rules", 0) \
    M(Bool, enable_push_topn_filtering_through_projection, true, "Whether to enable PushTopNFilteringThroughProjection rules", 0) \
    M(Bool, enable_push_topn_filtering_through_union, true, "Whether to enable PushTopNFilteringThroughUnion rules", 0) \
    M(Bool, enable_optimize_aggregate_memory_efficient, false, "Whether to enable OptimizeMemoryEfficientAggregation rules", 0) \
    M(Bool, enable_cascades_optimizer, true, "Whether to enable CascadesOptimizer", 0) \
    M(Bool, enable_iterative_rewriter, true, "Whether to enable InterativeRewriter", 0) \
    M(Float, multi_join_keys_correlated_coefficient, 0.8f, "Coefficient about multi join keys, the smaller the value, the smaller the estimated join cardnlity, do nothing when equals 1.0", 0) \
    M(Float, multi_agg_keys_correlated_coefficient, 0.9f, "Coefficient about multi agg keys, the smaller the value, the smaller the estimated agg cardnlity, do nothing when equals 1.0", 0) \
    M(Bool, enable_common_expression_sharing, true, "Whether to share common expression between steps", 0) \
    M(Bool, enable_common_expression_sharing_for_prewhere, true, "Whether to share common expression between steps and PREWHERE", 0) \
    M(Bool, enable_unalias_symbol_references, true, "Whether to enable unalias symbol references", 0) \
    M(UInt64, common_expression_sharing_threshold, 3, "The minimal cost to share a common expression, the cost is defined by (complexity * (occurrence - 1))", 0) \
    M(Bool, extract_bitmap_implicit_filter, true, "Whether to extract implicit filter for bitmap functions, e.g. for bitmapCount('1 | 2 & 3')(a, b), extract 'a in (1, 2, 3)'", 0) \
    M(Bool, enable_add_local_exchange, false, "Whether to add local exchange", 0) \
    M(Bool, enable_join_using_to_join_on, false, "Whether rewrite Join Using to Join On to make reordering possible", 0) \
    M(Bool, enable_ab_test, false, "Whether to open ab test for settings, If true, the settings for some queries are set in the ab_test_profile profile.", 0) \
    M(Float, ab_test_traffic_factor, 0.0f, "Proportion of queries that perform ab test, meaningful between 0 and 1", 0) \
    M(String, ab_test_profile, "default", "Profile name for ab test", 0) \
    M(Bool, optimize_json_function_to_subcolumn, false, "Whether to optimize json extract functions to subcolumn read", 0) \
    M(Bool, enable_element_mv_rows, false, "Whether enable element query calculate base rows and view rows", 0) \
    /** Optimizer relative settings, CBO, CTE, MagicSet, MV */ \
    M(Bool, enable_join_reorder, true, "Whether enable join reorder", 0) \
    M(UInt64, max_predicate_text_length, 5000, "Max length of predicate text", 0) \
    M(UInt64, cascades_optimizer_timeout, 10000, "Max running time of a single cascades optimizer in ms", 0) \
    M(UInt64 , max_graph_reorder_size, 6, "Max tables join order enum on graph", 0) \
    M(UInt64 , heuristic_join_reorder_enumeration_times, 3, "Heuristic times in CardinalityBased Join Reorder algorithm", 0) \
    M(Bool, enable_cbo, true, "Whether enable CBO", 0) \
    M(Bool, enable_cascades_pruning, true, "Whether enable cascades pruning", 0) \
    M(Bool, enum_replicate, true, "Enum replicate join", 0) \
    M(Bool, enum_repartition, true, "Enum repartition join", 0) \
    M(Bool, enum_replicate_no_stats, true, "Enum replicate join when statistics not exists", 0) \
    M(UInt64, max_replicate_build_size, 200000, "Max join build size, when enum replicate", 0) \
    M(UInt64, max_replicate_shuffle_size, 50000000, "Max join build size, when enum replicate", 0) \
    M(UInt64, parallel_join_threshold, 100000, "Parallel join right source rows threshold", 0) \
    M(UInt64, parallel_join_rows_batch_threshold, 4096, "Rows that concurrent hash join wait data reach, then to build hashtable or join block", 0) \
    M(Bool, add_parallel_after_join, false, "Add parallel after join", 0) \
    M(Bool, enforce_round_robin, false, "Whether add round robin exchange node", 0) \
    M(Bool, enable_shuffle_with_order, false, "Whether enable keep data order when shuffle", 0) \
    M(Bool, enable_merge_require_property, false, "Whether enable merge required property in aggregation", 0) \
    M(Bool, enable_join_graph_support_filter, true, "Whether enable join graph support filter", 0) \
    M(Bool, enable_equivalences, true, "Whether enable using equivalences when property match", 0) \
    M(Bool, enable_injective_in_property, false, "Whether enable using injective function when property match", 0) \
    M(Bool, enable_case_when_prop, false, "Whether enable case when prop", 0) \
    M(UInt64, max_expand_join_key_size, 3, "Whether enable using equivalences when property match", 0) \
    M(UInt64, max_expand_agg_key_size, 3, "Max allowed agg/window keys number when expand powerset when property match", 0) \
    M(Bool, enable_sharding_optimize, false, "Whether enable sharding optimization, eg. local join", 0) \
    M(Bool, enable_bucket_shuffle, false, "Whether enable bucket shuffle", 0) \
    M(Bool, enable_magic_set, true, "Whether enable magic set rewriting for join aggregation", 0) \
    M(Float, magic_set_filter_factor, 0.5f, "The minimum filter factor of magic set, used for early pruning", 0) \
    M(UInt64, magic_set_max_search_tree, 2, "The maximum table scans in magic set, used for early pruning", 0) \
    M(UInt64, magic_set_source_min_rows, 10000, "The minimum rows of source node in magic set, used for early pruning", 0) \
    M(Float, magic_set_rows_factor, 0.6f, "The minimum rows of source node in magic set, used for early pruning", 0) \
    M(Bool, enable_magic_set_cte, true, "Whether enable magic set rewriting build as cte", 0) \
    M(CTEMode, cte_mode, CTEMode::AUTO, "CTE mode: SHARED|INLINED|AUTO|ENFORCED", 0) \
    M(SpillMode, spill_mode, SpillMode::MANUAL, "SpillMode: MANUAL(default)|AUTO", 0) \
    M(UInt64, max_allowed_mem_size_in_join_spill, 512000000, "Max allowed memory-size(estimated) in join spill", 0) \
    M(Float, spill_triger_threshold, 0.7f, "Threshold to triger spill then memory usage reach a certain ratio of memory quota", 0) \
    M(Bool, enable_cte_property_enum, false, "Whether enumerate all possible properties for cte", 0) \
    M(Bool, enable_cte_common_property, true, "Whether search common property for cte", 0) \
    M(Bool, enable_windows_parallel, false, "Whether run windows in parallel", 0) \
    M(Bool, enable_view_based_query_rewrite, false, "Whether enable materialized view based rewriter for query, compatible for  enable_materialized_view_rewrite", 0) \
    M(Bool, enable_non_equijoin_reorder, true, "Whether enable no equi join reorder", 0) \
    M(Bool, enable_materialized_view_rewrite, false, "Whether enable materialized view based rewriter for query", 0) \
    M(Bool, enable_sync_materialized_view_rewrite, true, "Whether enable materialized view based rewriter for sync materialized view", 0) \
    M(Bool, enforce_materialized_view_rewrite, false, "Whether throw exception if materialized view is not applied", 0) \
    M(String, enable_push_partial_block_list, "", "Aggregate names who can push partial agg, split by ',' => axxx,bxxx,cxxx", 0) \
    M(Bool, enable_materialized_view_ast_rewrite, false, "Whether enable materialized view based rewriter for query", 0) \
    M(Bool, enable_materialized_view_rewrite_verbose_log, false, "Whether enable materialized view based rewriter for query", 0) \
    M(Bool, enable_materialized_view_empty_grouping_rewriting, true, "Whether enable materialized view based rewriter for query", 0) \
    M(Bool, enable_materialized_view_join_rewriting, true, "Whether enable materialized view based rewriter for query using join materialized views", 0) \
    M(Bool, enable_materialized_view_union_rewriting, false, "Whether enable materialized view based rewriter for query using union", 0) \
    M(Bool, enforce_materialized_view_union_rewriting, false, "Enforce enable materialized view based rewriter for query using union, used for testing", 0) \
    M(MaterializedViewConsistencyCheckMethod, materialized_view_consistency_check_method, MaterializedViewConsistencyCheckMethod::NONE, "The method to check whether a materialized view is consistent with the base table for a query", 0) \
    M(QueryDryRunMode, query_dry_run_mode, QueryDryRunMode::NONE, "Whether to choose a query debug mode, in order to skip some workloads", 0) \
    M(UInt64, max_plan_segment_num, 500, "maximum plan segments allowed, 0 means no restriction", 0)\
    M(Bool, force_create_foreign_key, false, "Whether to create inexistent foreign key when creating a table", 0) \
    M(Bool, enable_group_by_keys_pruning, false, "Whether to enable RBO -- group by keys pruning optimization", 0) \
    M(Bool, enable_eager_aggregation, false, "Whether to enable RBO -- eager aggregation optimization", 0) \
    M(Bool, only_push_agg_with_functions, false, "Only use eager aggregation with functions", 0) \
    M(Float, agg_push_down_threshold, 40.0f, "Which ratio is greater than threshold can be push down", 0) \
    M(Bool, agg_push_down_every_join, false, "Below every join can insert one agg instead of bottom jion", 0) \
    M(String, eager_agg_join_id_blocklist, "", "Which join in blocklist can't be push down through", 0) \
    M(String, eager_agg_join_id_whitelist, "", "Which join in blocklist can be push down through", 0) \
    M(Bool, enable_sum_if_to_count_if, false, "Whether enable rewrite sumIf to countIf", 0) \
    M(Bool, enable_eliminate_join_by_fk, false, "Whether to enable RBO -- eliminate join by fk optimization", 0) \
    M(Bool, enable_eliminate_complicated_pk_fk_join, false, "Whether to eliminate complicated join by fk optimization", 0) \
    M(Bool, enable_eliminate_complicated_pk_fk_join_without_top_join, false, "Whether to allow eliminate complicated join by fk pull through pass the multi-child node even if no top join", 0) \
    M(Bool, enable_filtered_pk_selectivity, 1, "Enable the selectivity of filtered pk table", 0) \
    M(Bool, execute_subquery_in_lambda, true, "Whether to execute subquery in lambda", 0) \
    M(Bool, early_execute_scalar_subquery, false, "Whether to early execute scalar subquery", 0) \
    M(Bool, early_execute_in_subquery, false, "Whether to early execute in subquery", 0) \
    M(String, prewhere_skip_functions, "", "A collection of functions which are not choosen as prewhere, use ',' to seperate", 0) \
    /** Complex query settings **/\
    M(Milliseconds, send_plan_segment_timeout_ms, 10000, "Default timeout for send plan segment by rpc", 0) \
    M(Bool, enable_distributed_stages, false, "Enable complex query mode to split plan to distributed stages", 0)\
    M(Bool, fallback_to_simple_query, false, "Enable fallback if there is any syntax error", 0)\
    M(Bool, send_plan_segment_by_brpc_join_per_stage, false, "Whether to send plan segment by BRPC and join async rpc request per stage", 0)\
    M(Bool, send_plan_segment_by_brpc_join_at_last, true, "Whether to send plan segment by BRPC and join async rpc request at last", 0)\
    /** Optimizer relative settings, statistics */ \
    M(Bool, create_stats_time_output, true, "Enable time output in create stats, should be disabled at regression test", 0) \
    M(Bool, statistics_forward_query, false, "Indicate whether this query is coming from another replica", 0)  \
    M(Bool, statistics_collect_histogram, true, "Enable histogram collection", 0) \
    M(Bool, statistics_collect_floating_histogram, true, "Collect histogram for float/double/Decimal columns", 0) \
    M(Bool, statistics_collect_floating_histogram_ndv, true, "Collect histogram ndv for float/double/Decimal columns", 0) \
    M(UInt64, statistics_collect_string_size_limit_for_histogram, 64, "Collect string histogram only for avg_size <= string_size_limit, since it's unnecessary to collect stats for text", 0) \
    M(UInt64, statistics_histogram_bucket_size, 250, "Default bucket size of histogram", 0) \
    M(UInt64, statistics_kll_sketch_log_k, DEFAULT_KLL_SKETCH_LOG_K, "Default logK parameter of kll_sketch in statistics", 0) \
    M(Bool, statistics_enable_async, false, "Collect stats use async mode", 0) \
    M(Bool, statistics_enable_sample, true, "Use sampling for statistics", 0) \
    M(UInt64, statistics_sample_row_count, 40'000'000, "Minimal row count for sampling", 0) \
    M(Float, statistics_sample_ratio, 0.001f, "Ratio for sampling", 0) \
    M(StatisticsAccurateSampleNdvMode, statistics_accurate_sample_ndv, StatisticsAccurateSampleNdvMode::AUTO, "Mode of accurate sample ndv to estimate full ndv", 0) \
    M(UInt64, statistics_accurate_sample_ndv_row_limit, 40'000'000, "Limit of accurate sample ndv sample row count, to limit create stats cost. 0 for unlimited", 0) \
    M(UInt64, statistics_batch_max_columns, 30, "Max column size in a batch when collecting stats", 0) \
    M(String, statistics_exclude_tables_regex, "", "Regex to exclude tables for statistics operations", 0) \
    M(Bool, statistics_if_not_exists, false, "Collect stats using if not exists mode", 0) \
    M(Bool, statistics_simplify_histogram, false, "Reduce buckets of histogram with simplifying", 0) \
    M(Float, statistics_simplify_histogram_ndv_density_threshold, 0.2f, "Histogram simplifying threshold for ndv", 0) \
    M(Float, statistics_simplify_histogram_range_density_threshold, 0.2f, "Histogram simplifying threshold for range", 0) \
    M(Bool, statistics_expand_to_current, true, "Expand Date/Date32/DateTime/DateTime64 columns stats to current timestamp", 0) \
    M(UInt64, statistics_current_timestamp, 0, "Timestamp used for statistics_expand_to_current, 0 to use now(), for testing purpose", 0) \
    M(UInt64, statistics_expand_to_current_threshold_days, 31, "If abs(stats_timestamp - stats_column_max) is within this threshold, we will expand this column", 0) \
    M(Float, statistics_expand_to_current_histogram_ratio, 0.10f, "For histogram, only expand last buckets containing rows with this ratio", 0) \
    M(StatisticsCachePolicy, statistics_cache_policy, StatisticsCachePolicy::Default, "Cache policy for stats command and SQLs: (default|cache|catalog)", 0) \
    M(Bool, statistics_return_row_count_if_empty, false, "Deprecated settings", 0) \
    M(Bool, statistics_use_hive_metastore, false, "Deprecated Settings", 0) \
    M(Bool, statistics_collect_in_partitions, false, "Collect partitioned stats", 0) \
    M(UInt64, statistics_max_partitions_in_a_batch, 1000, "Max parallel size of partitions in single batch when collect partitioned stats", 0) \
    M(Int64, statistics_ignore_modified_timestamp_older_than, 0, "Ignore partitions whose modified_time older than this Unix timestamp. 0 for unlimited, negative value for now() - abs(value)", 0) \
    M(UInt64, statistics_max_partitions, 0, "Max partitions in total to collect partitioned stats, 0 for unlimited", 0) \
    M(Bool, statistics_query_cnch_parts_for_row_count, true, "Use cnch parts instead of count(*) for row count to speed up test", 0) \
    /** Optimizer relative settings, cost model and estimation */ \
    M(Float, cost_calculator_cpu_cost_ratio, 0.74f, "Table scan cost weight for cost calculator", 0) \
    M(Float, cost_calculator_mem_cost_ratio, 0.16f, "Table scan cost weight for cost calculator", 0) \
    M(Float, cost_calculator_net_cost_ratio, 1.0f, "Table scan cost weight for cost calculator", 0) \
    M(Float, cost_calculator_table_scan_weight, 3.8f, "Table scan cost weight for cost calculator", 0) \
    M(Float, cost_calculator_aggregating_weight, 7.0f, "Aggregate output weight for cost calculator", 0) \
    M(Float, cost_calculator_join_probe_weight, 0.5f, "Join probe side weight for cost calculator", 0) \
    M(Float, cost_calculator_join_build_weight, 1.5f, "Join build side weight for cost calculator", 0) \
    M(Float, cost_calculator_join_output_weight, 0.5f, "Join output weight for cost calculator", 0) \
    M(Float, cost_calculator_cte_weight, 1.0f, "CTE output weight for cost calculator", 0) \
    M(Float, cost_calculator_cte_weight_for_join_build_side, 1.3f, "Join build side weight for cost calculator", 0) \
    M(Float, cost_calculator_projection_weight, 0.1f, "CTE output weight for cost calculator", 0) \
    M(Bool, cost_calculator_use_size, true, "Whether use byte size to calc cost", 0) \
    M(Bool, cost_calculator_use_size_in_join, true, "Whether use byte size to calc cost in join", 0) \
    M(Float, cost_calculator_byte_size_weight, 1.0f, " Byte size weight for cost calculator", 0) \
    M(Float, stats_estimator_join_filter_selectivity, 0.5f, "Join filter selectivity", 0) \
    M(Bool, stats_estimator_join_use_histogram, true, "Estimate join use histogram", 0) \
    M(Float, stats_estimator_anti_join_filter_coefficient, 0.6f, "Anti Join filter coefficient", 0) \
    M(Float, stats_estimator_first_agg_key_filter_coefficient, 0.3f, "First agg key coefficient", 0) \
    M(Float, stats_estimator_remaining_agg_keys_filter_coefficient, 1.5f, "Remaining agg key coefficient", 0) \
    M(Float, stats_estimator_unknown_filter_selectivity, 0.25f, "Join filter selectivity", 0) \
    M(Float, stats_estimator_unknown_in_filter_selectivity, 0.5f, "In filter selectivity", 0) \
    M(Float, stats_estimator_like_selectivity, 0.15f, "Like filter selectivity", 0) \
    M(Bool, enable_estimate_without_symbol_statistics, false, "Try to estimiate cardinality even if no symbol statistics", 0) \
    M(Bool, enable_left_deep_join_reorder, false, "Try to do join reorder without accurate statistics", 0) \
    M(Bool, enable_pk_fk, true, "Whether enable PK-FK join estimation", 0) \
    M(Bool, enable_real_pk_fk, true, "Whether enable Real PK-FK join estimation", 0) \
    M(Float, pk_selectivity, 1.0f, "PK selectivity for join estimation", 0) \
    /* Outfile related Settings */ \
    M(Bool, enable_distributed_output, false, "Each worker is allowed to output query results to a file separately", 0) \
    /** Settings for Map */ \
    M(Bool, allow_map_access_without_key, true, "Allow access map column without providing key", 0) \
    M(Bool, offloading_with_query_plan, false, "utilize query plan to offload the computation completely to worker", 0) \
    /** Sample setttings */ \
    M(Bool, enable_sample_by_range, false, "Sample by range if it is true", 0) \
    M(Bool, enable_deterministic_sample_by_range, false, "Deterministic sample by range if it is true", 0) \
    M(Bool, uniform_sample_by_range, false, "Sample by range with uniform mode", 0) \
    M(Bool, ensure_one_mark_in_part_when_sample_by_range, true, "Sample by range will ensure at least a mark is sampled in each part, otherwise will do sample on parts when necessary", 0) \
    M(Bool, enable_final_sample, false, "Sample from result rows if it is true", 0) \
    M(Bool, uniform_final_sample, false, "Final sample with uniform mode", 0)\
    \
    /** Just for compatible, maybe removed or implemented later */ \
    M(UInt64, max_in_value_list_to_pushdown, 10000, "Max size of in value list in filter", 0) \
    M(UInt64, max_query_cpu_seconds, 0, "Limit the maximum amount of CPU resources such a query segment can consume.", 0) \
    M(UInt64, max_distributed_query_cpu_seconds, 0, "Limit the maximum amount of CPU resources such a distribute query can consume.", 0) \
    M(Float, streaming_agg_local_ratio, 0.25f, "The ratio of local streaming agg, 0-all streaming, 1-all local merged", 0) \
    M(Bool, optimize_read_in_partition_order, false, "In optimize_read_in_order mode, whether to read parts partition-by-partition if applicable, it will also delay inverted index evaluation till pipeline execution", 0) \
    M(UInt64, early_limit_for_map_virtual_columns, 0, "Enable early limit while quering _map_column_keys column", 0)\
    M(Bool, force_read_in_partition_order, 0, "Similar to optimize_read_in_partition_order, but throw an exception if it cannot be applied to the query, mainly for testing", 0) \
    M(Bool, check_identifier_begin_valid, true, "Whether to check identifier", 0) \
    M(Bool, ignore_array_join_check_in_join_on_condition, false, "Ignore array-join function check in join on condition", 0) \
    M(Bool, bsp_mode, false, "If enabled, query will execute in bsp mode", 0) \
    M(String, exchange_shuffle_method_name, "cityHash64", "Shuffle method name used in exchange", 0) \
    M(UInt64, distributed_max_parallel_size, 1000, "Max distributed execution parallel size", 0) \
    M(Bool, log_optimizer_run_time, false, "Whether Log optimizer runtime", 0) \
    M(Bool, log_query_plan, 0, "Log json format query plan to the system query_log table.", 0) \
    M(LogExplainAnalyzeType, log_explain_analyze_type, LogExplainAnalyzeType::NONE, "Log explain analyze result. Type: NONE|QUERY_PIPELINE|AGGREGATED_QUERY_PIPELINE.", 0) \
    M(UInt64, max_plannode_count, 200, "The max plannode count", 0) \
    M(Bool, enable_plan_cache, false, "Whether enable plan cache", 0) \
    M(Bool, enable_transactional_query_cache, true, "Enable transactional query cache for CNCH engine table", IMPORTANT) \
    M(UInt64, spill_buffer_bytes_before_external_group_by, 10485760, "Agg memory buffer threshold when the spill trigger condition is reached, default 10Mb", 0) \
    M(Bool, enable_lc_group_by_opt, false, "Whether enable single lowcardinality column group by optimize", 0) \
    M(DialectType, dialect_type, DialectType::CLICKHOUSE, "Dialect type, e.g. CLICKHOUSE, ANSI, MYSQL", 0) \
    M(Bool, prefer_alias_if_column_name_is_ambiguous, false, "If source columns are ambiguous, prefer to use alias, for MySQL compatibility", 0) \
    M(Bool, only_full_group_by, true, "If the ONLY_FULL_GROUP_BY is enabled (which it is by default), rejects queries for which the select list, HAVING condition, or ORDER BY list refer to nonaggregated columns that are neither named in the GROUP BY clause nor are functionally dependent on them.", 0) \
    M(Bool, enable_ab_index_optimization, true, "Optimize ab version by reading Bitmap", 0)\
    M(Int64, partition_by_monotonicity_hint, 0, "Hint on whether partition by expression is a monotonic function or not, e.g., '(toYYYYMMDD(ts), toHour(ts))' is a monotonic non-decreasing function. 0 means unknown, Positive means monotonic non-decrasing, Negative means monotonic non-increasing", 0) \
    M(Bool, allow_extended_type_conversion, false, "When enabled, implicit type conversion is allowed for more input types(e.g. UInt64 & Ints, Decimal & Float, Float & Int64)", 0) \
    M(Bool, enable_implicit_arg_type_convert, false, "Eable implicit type conversion for functions", 0) \

#define MAKE_OPTIMIZER_OBSOLETE(M, TYPE, NAME, DEFAULT) \
    M(TYPE, NAME, DEFAULT, "Obsolete setting, does nothing.", BaseSettingsHelpers::Flags::OBSOLETE)

#define OBSOLETE_OPTIMIZER_SETTINGS(M, ALIAS) \
    /** Obsolete settings that do nothing now but left for compatibility reasons. Remove them or implement them when you have free time. */ \
    MAKE_OPTIMIZER_OBSOLETE(M, Bool, enable_two_stages_prewhere, false) \
    /** End of OBSOLETE_OPTIMIZER_SETTINGS */ \

#define ALL_OPTIMIZER_SETTINGS(M, ALIAS) \
LIST_OF_OPTIMIZER_SETTINGS(M, ALIAS) \
OBSOLETE_OPTIMIZER_SETTINGS(M, ALIAS) \

DECLARE_SETTINGS_TRAITS(OptimizerSettingsTraits, ALL_OPTIMIZER_SETTINGS)


struct OptimizerSettings : public BaseSettings<OptimizerSettingsTraits>, public IHints<2>
{
    OptimizerSettings() = default;
    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);

    std::unordered_map<String, String> dumpToMap() const
    {
        std::unordered_map<String, String> res;
        for (const auto & field : *this)
        {
            res.emplace(field.getName(), field.getValueString());
        }
        return res;
    }

    void dumpToJSON(Poco::JSON::Object & dumpJson) const
    {
        for (const auto & setting : all(SKIP_UNCHANGED))
        {
            auto name = setting.getName();
            auto value = setting.getValueString();
            dumpJson.set(name, value);
        }
    }

    std::vector<String> getAllRegisteredNames() const override;

    void set(std::string_view name, const Field & value) override;
};

using OptimizerSettingsPtr = std::shared_ptr<OptimizerSettings>;

}
