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

/** These settings represent fine tunes for internal details of query optimizer 
 * and should not be changed by the user without a reason.
  */
#define LIST_OF_COORDINATION_SETTINGS(M, ALIAS) \
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
    M(UInt64, distributed_query_wait_exception_ms, 2000,"Wait final planSegment exception from segmentScheduler.", 0) \
    M(Bool, enable_wait_for_post_processing, false, "Whether a query needs to wait for post processing rpcs done before end", 0) \
    M(UInt64, wait_for_post_processing_timeout_ms, 1000, "Timeout for waiting post processing rpc from workers.", 0) \
    M(UInt64, exchange_wait_accept_max_timeout_ms, 20000, "Exchange receiver wait accept max timeout ms",0) \
    M(UInt64, exchange_unordered_output_parallel_size, 8, \
        "The num of exchange sink for unorder exchange, ingoned if exchange need keep data order ", 0) \
    M(Bool, exchange_enable_force_keep_order, false, "Force exchange keep data order", 0) \
    M(Bool, exchange_enable_keep_order_parallel_shuffle, false, "Whether enable parallel shuffle when exchange need keep order", 0) \
    /** Runtime Filter settings */ \
    M(UInt64, wait_runtime_filter_timeout, 1000, "Execute filter wait for runtime filter timeout ms", 0) \
    /** Debug settings */ \
    M(Bool, log_segment_profiles, false, "Log profile of each segment info including runtime and planning information.", 0) \
    M(Bool, report_segment_profiles, false, "Report plan segment profile to coordinator.", 0) \
    /** Optimizer relative settings, CBO, CTE, MagicSet, MV */ \
    M(QueryDryRunMode, query_dry_run_mode, QueryDryRunMode::NONE, "Whether to choose a query debug mode, in order to skip some workloads", 0) \
    /** Complex query settings **/\
    M(Milliseconds, send_plan_segment_timeout_ms, 10000, "Default timeout for send plan segment by rpc", 0) \

DECLARE_SETTINGS_TRAITS(OptimizerSettingsTraits, LIST_OF_COORDINATION_SETTINGS)


struct OptimizerSettings : public BaseSettings<OptimizerSettingsTraits>
{
    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);
};

using OptimizerSettingsPtr = std::shared_ptr<OptimizerSettings>;

}
