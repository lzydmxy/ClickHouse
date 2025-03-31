#pragma once

#include <base/types.h>
#include <Core/Defines.h>
#include <Core/BaseSettings.h>
#include <Core/SettingsEnums.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <IO/WriteBufferFromString.h>

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

DECLARE_SETTING_ENUM(CTEMode)


constexpr UInt64 RUNTIME_FILTER_BLOOM_BUILD_THRESHOLD = 2048000; // Default threshold of right table to build bloom filter
constexpr UInt64 RUNTIME_FILTER_IN_BUILD_THRESHOLD = 1024; // Default threshold of right table to build value set filter


/** These settings represent fine tunes for internal details of query optimizer
 * and should not be changed by the user without a reason.
  */
#define LIST_OF_OPTIMIZER_SETTINGS(M, ALIAS) \
    /** General extension settings */ \
    M(Bool, log_normalized_query_plan_hash, 0, "Log json format query plan to the system query_log table.", 0) \
    M(Bool, log_query_exchange, false, "Log query exchange metric.", 0) \
    /** Coordinator settings*/ \
    M(SchedulerMode, scheduler_mode, SchedulerMode::RANDOM, "scheduler shard mode: random/first_order/random_order/cpu_rank/memory_rank", 0) \
    M(UInt64, operator_profile_receive_timeout, 3000, "Max waiting time for operator profile in ms", 0) \
    M(UInt64, push_queue_timeout_millseconds, 10, "Timeout millseconds of push profile or others to queue.", 0) \
    /** Query optimizer relative settings */ \
    M(Bool, enable_optimizer, true, "Whether enable query optimizer", 0) \
    M(Bool, enable_legacy_optimizer, false, "Whether enable query optimizer", 0) \
    M(UInt64, exchange_buffer_send_threshold_in_bytes, 1000000, "The minimum bytes when exchange will flush send buffer ", 0) \
    M(UInt64, exchange_buffer_send_threshold_in_row, 65505, "The minimum row num when exchange will flush send buffer", 0) \
    M(Bool, exchange_enable_force_remote_mode, false, "Force exchange data transfer through network", 0) \
    M(Bool, exchange_force_use_buffer, false, "Force exchange use buffer as possible", 0) \
    M(Bool, enable_prune_source_plan_segment, false, "Whether prune source plan segment", 0) \
    /** Exchange settings */ \
    M(UInt64, exchange_timeout_ms, 1000000, "Exchange request timeout ms",0) \
    M(UInt64, exchange_queue_bytes, 209715200, "Queue size(bytes) for exchange queue, 0 means disable", 0) \
    M(Bool, exchange_use_query_memory_tracker, true, "Use query-level memory tracker", 0) \
    M(UInt64, exchange_parallel_size, 1, "Exchange parallel size", 0) \
    M(UInt64, distributed_query_wait_exception_ms, 2000,"Wait final planSegment exception from segmentScheduler.", 0) \
    M(Bool, enable_wait_for_post_processing, false, "Whether a query needs to wait for post processing rpcs done before end", 0) \
    M(UInt64, wait_for_post_processing_timeout_ms, 1000, "Timeout for waiting post processing rpc from workers.", 0) \
    M(UInt64, exchange_wait_accept_max_timeout_ms, 20000, "Exchange receiver wait accept max timeout ms",0) \
    M(UInt64, exchange_unordered_output_parallel_size, 8, \
        "The num of exchange sink for unorder exchange, ingoned if exchange need keep data order ", 0) \
    M(Bool, exchange_enable_force_keep_order, false, "Force exchange keep data order", 0) \
    M(Bool, exchange_enable_keep_order_parallel_shuffle, false, "Whether enable parallel shuffle when exchange need keep order", 0) \
    M(UInt64, disk_shuffle_advisory_partition_size, 104857600, "Disk shuffle files's advisory partition size(including all files in a partition), used by partition coalescing", 0) \
    M(Bool, enable_disk_shuffle_partition_coalescing, true, "If enabled, sheduler will try to coalesce overly-small partitions, thus avoid small plan segments and I/O waste", 0) \
    M(Bool, enable_batch_send_plan_segment, true, "Whether enable combined sending plan segments to reduce rpc calls", 0) \
    M(UInt64, exchange_remote_receiver_queue_size, 10, "Queue size for remote exchange receiver",0) \
    M(UInt64, exchange_stream_max_buf_size, 20971520, "Default 20M, -1 means no limit", 0) \
    M(Bool, exchange_enable_block_compress, true, "Whether enable exchange block compress ", 0) \
    M(UInt64, exchange_local_receiver_queue_size, 30, "Queue size for local exchange receiver",0) \
    M(UInt64, exchange_multi_path_receiver_queue_size, 20, "Queue size for multi path exchange receiver", 0) \
    M(UInt64, exchange_stream_back_pressure_max_wait_ms, 0, "Default 0, 0 means no control", 0) \
    M(Bool, exchange_enable_multipath_reciever, true, "Whether enable exchange new mode ", 0) \
    M(UInt64, exchange_source_pipeline_threads, 16, "Recommend number of threads for pipeline which reading data from exchange, ingoned if exchange need keep data order", 0) \
    /** Runtime Filter settings */ \
    M(UInt64, wait_runtime_filter_timeout, 1000, "Execute filter wait for runtime filter timeout ms", 0) \
    M(Bool, enable_range_cover, true, "Whether use range rather than bloom or values set for runtime filter", 0) \
    M(UInt64, clean_rf_time_limit, 300000, "Threshold to clean runtime filters in manager to prevent memory leak", 0) \
    M(Bool, enable_rewrite_bf_into_prewhere, true, "Whether enable pushdown runtime filter to prewhere for join", 0) \
    M(UInt64, runtime_filter_bloom_build_threshold, RUNTIME_FILTER_BLOOM_BUILD_THRESHOLD, "The threshold of right table to build bloom filter", 0) \
    M(UInt64, runtime_filter_in_build_threshold, RUNTIME_FILTER_IN_BUILD_THRESHOLD, "The threshold of right table to build value set filter", 0) \
    /** Optimizer join settings */ \
    M(Bool, enforce_all_join_to_any_join, false, "Whether enforce all join to any join", 0) \
    M(Bool, enable_nested_loop_join, false, "Whether enable nest loop join for outer join with filter", 0)\
    M(Bool, use_grace_hash_only_repartition, false, "Only use grace hash join when exchange type is repartition", 0) \
    M(UInt64, grace_hash_join_left_side_parallel, 1, "Initial number of grace hash join left side parallel", 0) \
    /** Debug settings */ \
    M(Bool, log_segment_profiles, false, "Log profile of each segment info including runtime and planning information.", 0) \
    M(Bool, report_segment_profiles, false, "Report plan segment profile to coordinator.", 0) \
    /** */ \
    M(Bool, convert_to_right_type_for_in_subquery, true, "For IN subquery, whether convert arguments to the right type", 0) \
    /** Optimizer relative settings, Plan build and RBO */ \
    M(Bool, enable_implicit_type_conversion, true, "Whether enable implicit type conversion for JOIN, Set operation, IN subquery", 0) \
    M(Bool, enable_subcolumn_optimization_through_union, true, "Whether enable sub column optimization through set operation.", 0) \
    M(Bool, optimize_json_function_to_subcolumn, false, "Whether to optimize json extract functions to subcolumn read", 0) \
    /** Optimizer relative settings, CBO, CTE, MagicSet, MV */ \
    M(CTEMode, cte_mode, CTEMode::AUTO, "CTE mode: SHARED|INLINED|AUTO|ENFORCED", 0) \
    M(QueryDryRunMode, query_dry_run_mode, QueryDryRunMode::NONE, "Whether to choose a query debug mode, in order to skip some workloads", 0) \
    M(Bool, enable_shuffle_with_order, false, "Whether enable keep data order when shuffle", 0) \
    M(Bool, execute_subquery_in_lambda, true, "Whether to execute subquery in lambda", 0) \
    M(Bool, early_execute_scalar_subquery, false, "Whether to early execute scalar subquery", 0) \
    M(Bool, early_execute_in_subquery, false, "Whether to early execute in subquery", 0) \
    /** Complex query settings **/\
    M(Milliseconds, send_plan_segment_timeout_ms, 10000, "Default timeout for send plan segment by rpc", 0) \
    M(Bool, enable_distributed_stages, false, "Enable complex query mode to split plan to distributed stages", 0)\
    M(Bool, fallback_to_simple_query, false, "Enable fallback if there is any syntax error", 0)\
    M(Bool, send_plan_segment_by_brpc_join_per_stage, false, "Whether to send plan segment by BRPC and join async rpc request per stage", 0)\
    M(Bool, send_plan_segment_by_brpc_join_at_last, true, "Whether to send plan segment by BRPC and join async rpc request at last", 0)\
    /** Just for compatible, maybe removed or implemented later */ \
    M(UInt64, max_query_cpu_seconds, 0, "Limit the maximum amount of CPU resources such a query segment can consume.", 0) \
    M(UInt64, max_distributed_query_cpu_seconds, 0, "Limit the maximum amount of CPU resources such a distribute query can consume.", 0) \
    M(Float, streaming_agg_local_ratio, 0.25, "The ratio of local streaming agg, 0-all streaming, 1-all local merged", 0) \
    M(OverflowMode, timeout_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    M(Bool, optimize_read_in_partition_order, false, "In optimize_read_in_order mode, whether to read parts partition-by-partition if applicable, it will also delay inverted index evaluation till pipeline execution", 0) \
    M(UInt64, early_limit_for_map_virtual_columns, 0, "Enable early limit while quering _map_column_keys column", 0)\
    M(Bool, enable_sample_by_range, false, "Sample by range if it is true", 0) \
    M(Bool, enable_deterministic_sample_by_range, false, "Deterministic sample by range if it is true", 0) \
    M(Bool, force_read_in_partition_order, 0, "Similar to optimize_read_in_partition_order, but throw an exception if it cannot be applied to the query, mainly for testing", 0) \
    M(Bool, check_identifier_begin_valid, true, "Whether to check identifier", 0) \
    M(Bool, ignore_array_join_check_in_join_on_condition, false, "Ignore array-join function check in join on condition", 0) \
    M(Bool, bsp_mode, false, "If enabled, query will execute in bsp mode", 0) \
    M(String, exchange_shuffle_method_name, "cityHash64V2", "Shuffle method name used in exchange", 0) \
    M(UInt64, distributed_max_parallel_size, false, "Max distributed execution parallel size", 0) \
    M(Bool, enable_memory_catalog, false, "Enable memory catalog for unittest", 0) \
    M(UInt64, max_plan_segment_num, 500, "maximum plan segments allowed, 0 means no restriction", 0)\
    M(Bool, log_optimizer_run_time, false, "Whether Log optimizer runtime", 0) \
    M(Bool, log_query_plan, 0, "Log json format query plan to the system query_log table.", 0) \
    M(UInt64, iterative_optimizer_timeout, 10000, "Max running time of a single iterative optimizer in ms", 0) \
    M(LogExplainAnalyzeType, log_explain_analyze_type, LogExplainAnalyzeType::NONE, "Log explain analyze result. Type: NONE|QUERY_PIPELINE|AGGREGATED_QUERY_PIPELINE.", 0) \
    M(UInt64, max_plannode_count, 200, "The max plannode count", 0) \
    M(Bool, enable_plan_cache, false, "Whether enable plan cache", 0) \

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


struct OptimizerSettings : public BaseSettings<OptimizerSettingsTraits>
{
    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);
    /// TODO: Need dump to map and send to rpc service
    std::unordered_map<String, String> dumpToMap() const;
};

using OptimizerSettingsPtr = std::shared_ptr<OptimizerSettings>;

}
