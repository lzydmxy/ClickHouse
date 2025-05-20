#include <Query/Statistics/SubqueryHelper.h>

#include <Core/SettingsEnums.h>

namespace DB::QueryStatistics
{

ContextMutablePtr SubqueryHelper::createQueryContext(ContextPtr context)
{
    auto query_context = createContextForSubQuery(context, "_create_stats_internal_");

    SettingsChanges changes;
    changes.emplace_back("dialect_type", "CLICKHOUSE");
    changes.emplace_back("database_atomic_wait_for_drop_and_detach_synchronously", true);
    changes.emplace_back("enable_deterministic_sample_by_range", true);
    changes.emplace_back("uniform_sample_by_range", true);
    changes.emplace_back("ensure_one_mark_in_part_when_sample_by_range", false);
    changes.emplace_back("insert_distributed_sync", true);
    changes.emplace_back("aggregate_functions_null_for_empty", false);
    changes.emplace_back("empty_result_for_aggregation_by_empty_set", false);
    changes.emplace_back("enable_final_sample", false);
    changes.emplace_back("statistics_return_row_count_if_empty", false);
    changes.emplace_back("limit", 0);
    changes.emplace_back("offset", 0);
    changes.emplace_back("force_primary_key", 0);
    changes.emplace_back("force_index_by_date", 0);
    changes.emplace_back("optimize_skip_unused_shards", 1);
    changes.emplace_back("join_use_nulls", 1);

    // manual xml config should have higher priority, so apply code changes first
    query_context->applySettingsChanges(changes);

    // NOTE: <statistics_internal_sql> can only control the behaviour of internal sqls
    // normal statistics flags are controled by default/user profile
    // TODO(gouguilin): bc, fix me
#if 0
    const String profile_name = "statistics_internal_sql";
    if (query_context->hasProfile(profile_name))
    {
        query_context->setCurrentProfile(profile_name);
    }
#endif

    return query_context;
}

}
