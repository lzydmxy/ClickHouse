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


/** These settings represent fine tunes for internal details of query optimizer
 * and should not be changed by the user without a reason.
  */
#define LIST_OF_COORDINATION_SETTINGS(M, ALIAS) \
    /** General extension settings */ \
    M(Bool, log_normalized_query_plan_hash, 0, "Log json format query plan to the system query_log table.", 0) \
    M(Bool, log_query_exchange, false, "Log query exchange metric.", 0) \
    /** Coordinator settings*/ \
    M(SchedulerMode, scheduler_mode, SchedulerMode::RANDOM, "scheduler shard mode: random/first_order/random_order/cpu_rank/memory_rank", 0) \
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
    /** Runtime Filter settings */ \
    M(UInt64, wait_runtime_filter_timeout, 1000, "Execute filter wait for runtime filter timeout ms", 0) \
    M(Bool, enable_range_cover, true, "Whether use range rather than bloom or values set for runtime filter", 0) \
    M(UInt64, clean_rf_time_limit, 300000, "Threshold to clean runtime filters in manager to prevent memory leak", 0) \
    M(Bool, enable_rewrite_bf_into_prewhere, true, "Whether enable pushdown runtime filter to prewhere for join", 0) \
    /** Debug settings */ \
    M(Bool, log_segment_profiles, false, "Log profile of each segment info including runtime and planning information.", 0) \
    M(Bool, report_segment_profiles, false, "Report plan segment profile to coordinator.", 0) \
    /** Optimizer relative settings, CBO, CTE, MagicSet, MV */ \
    M(QueryDryRunMode, query_dry_run_mode, QueryDryRunMode::NONE, "Whether to choose a query debug mode, in order to skip some workloads", 0) \
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

#define MAKE_COORDINATION_OBSOLETE(M, TYPE, NAME, DEFAULT) \
    M(TYPE, NAME, DEFAULT, "Obsolete setting, does nothing.", BaseSettingsHelpers::Flags::OBSOLETE)

#define OBSOLETE_COORDINATION_SETTINGS(M, ALIAS) \
    /** Obsolete settings that do nothing now but left for compatibility reasons. Remove them or implement them when you have free time. */ \
    MAKE_COORDINATION_OBSOLETE(M, Bool, enable_two_stages_prewhere, false) \
    /** End of OBSOLETE_COORDINATION_SETTINGS */ \

#define ALL_COORDINATION_SETTINGS(M, ALIAS) \
LIST_OF_COORDINATION_SETTINGS(M, ALIAS) \
OBSOLETE_COORDINATION_SETTINGS(M, ALIAS) \

DECLARE_SETTINGS_TRAITS(OptimizerSettingsTraits, ALL_COORDINATION_SETTINGS)


struct OptimizerSettings : public BaseSettings<OptimizerSettingsTraits>
{
    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);
    /// TODO: Need dump to map and send to rpc service
    std::unordered_map<String, String> dumpToMap() const;
};

using OptimizerSettingsPtr = std::shared_ptr<OptimizerSettings>;

}
