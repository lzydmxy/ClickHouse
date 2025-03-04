#pragma once

#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/IntersectOrExceptStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/RollupStep.h>

#include <Query/Processors/QueryPlan/AnyStepExt.h>
#include <Query/Processors/QueryPlan/ApplyStepExt.h>
#include <Query/Processors/QueryPlan/IntermediateResultCacheStepExt.h>
#include <Query/Processors/QueryPlan/JoinStepExt.h>
#include <Query/Processors/QueryPlan/MultiJoinStepExt.h>
#include <Query/Processors/QueryPlan/UnionStepExt.h>

#include <Query/Processors/QueryPlan/FilterStepExt.h>
#include <Query/Processors/QueryPlan/AssignUniqueIdStepExt.h>
#include <Query/Processors/QueryPlan/ExpandStepExt.h>
#include <Query/Processors/QueryPlan/MarkDistinctStepExt.h>
#include <Query/Processors/QueryPlan/SettingQuotaAndLimitsStepExt.h>
#include <Query/Processors/QueryPlan/TopNFilteringStepExt.h>
#include <Query/Processors/QueryPlan/ExchangeStepExt.h>
#include <Query/Processors/QueryPlan/LocalExchangeStepExt.h>
#include <Query/Processors/QueryPlan/RemoteExchangeSourceStepExt.h>

namespace DB
{
using QueryPlanStepShardPtr = std::shared_ptr<IQueryPlanStep>;


#define APPLY_QUERY_PLAN_STEP_TYPES(M) \
    M(CubeStep) \
    M(ExtremesStep) \
    M(RollupStep) \
    M(JoinStepExt) \
    M(FilledJoinStep) \
    M(MultiJoinStepExt) \
    M(UnionStepExt) \
    M(IntermediateResultCacheStepExt) \
    M(CreatingSetStep) \
    M(CreatingSetsStep) \
    M(IntersectOrExceptStep) \
    M(ApplyStepExt) \
    M(AnyStepExt) \
    M(AssignUniqueIdStepExt) \
    M(ExpandStepExt) \
    M(MarkDistinctStepExt) \
    M(SettingQuotaAndLimitsStepExt) \
    M(TopNFilteringStepExt) \
    M(ExchangeStepExt) \
    M(LocalExchangeStepExt) \
    M(RemoteExchangeSourceStepExt) \
    M(OffsetStep) \
    M(AggregatingProjectionStep) \
    M(FilterStepExt) \
    M(AggregatingStep) \
    M(MergingAggregatedStep) \
    M(WindowStep) \
    M(SortingStep) \
    M(FillingStep) \
    M(LimitStep)

#define ENUM_QUERY_PLAN_STEP_TYPE(ITEM) ITEM,
enum class QueryPlanStepType : UInt8
{
    APPLY_QUERY_PLAN_STEP_TYPES(ENUM_QUERY_PLAN_STEP_TYPE) UNDEFINED,
};
#undef ENUM_QUERY_PLAN_STEP_TYPE

inline String toString(QueryPlanStepType type)
{
    switch (type)
    {
#define ENUM_QUERY_PLAN_STEP_TYPE(ITEM) \
    case QueryPlanStepType::ITEM: \
        return #ITEM;
        APPLY_QUERY_PLAN_STEP_TYPES(ENUM_QUERY_PLAN_STEP_TYPE)
#undef ENUM_QUERY_PLAN_STEP_TYPE
        default:
            return "UNDEFINED";
    }
}

#define CHECK_AND_RETURN_QUERY_PLAN_STEP_TYPE(type) \
if (auto casted_query_plan_step = std::dynamic_pointer_cast<type>(query_plan_step)) \
{ \
    return QueryPlanStepType::type; \
}

inline QueryPlanStepType getQueryPlanStepType(const QueryPlanStepShardPtr & query_plan_step)
{
    APPLY_QUERY_PLAN_STEP_TYPES(CHECK_AND_RETURN_QUERY_PLAN_STEP_TYPE)
    return QueryPlanStepType::UNDEFINED;
}
#undef CHECK_AND_RETURN_QUERY_PLAN_STEP_TYPE


class QueryPlanStepHelper
{
public:
    QueryPlanStepHelper() = default;
    ~QueryPlanStepHelper() = default;

    static bool isLogicalQueryPlanStep(const QueryPlanStepShardPtr & query_plan_step) { return !isPhysicalQueryPlanStep(query_plan_step); }

    static bool isPhysicalQueryPlanStep(const QueryPlanStepShardPtr & query_plan_step)
    {
        if (auto join_step_ext = std::dynamic_pointer_cast<JoinStepExt>(query_plan_step))
            return join_step_ext->getDistributionType() != DistributionType::UNKNOWN;
        if (auto casted_query_plan_step = std::dynamic_pointer_cast<MultiJoinStepExt>(query_plan_step))
            return false;

        return true;
    }

    static QueryPlanStepShardPtr copyQueryPlanStep(const QueryPlanStepShardPtr & query_plan_step, ContextPtr context)
    {
        if (auto step_ptr = std::dynamic_pointer_cast<OffsetStep>(query_plan_step))
            return std::make_shared<OffsetStep>(step_ptr->input_streams[0], step_ptr->offset);
        if (auto join_step_ptr = std::dynamic_pointer_cast<JoinStepExt>(query_plan_step))
            return join_step_ptr->copy(context);
        if (auto union_step_ptr = std::dynamic_pointer_cast<UnionStepExt>(query_plan_step))
            return union_step_ptr->copy(context);
        if (auto any_step = std::dynamic_pointer_cast<AnyStepExt>(query_plan_step))
            return any_step->copy(context);
        if (auto multi_join_step_ptr = std::dynamic_pointer_cast<MultiJoinStepExt>(query_plan_step))
            return multi_join_step_ptr->copy(context);
        if (auto intermediate_result_cache_step_ptr = std::dynamic_pointer_cast<IntermediateResultCacheStepExt>(query_plan_step))
            return intermediate_result_cache_step_ptr->copy(context);
        if (auto apply_step_ptr = std::dynamic_pointer_cast<ApplyStepExt>(query_plan_step))
            return apply_step_ptr->copy(context);
        if (auto fill_join_step_ptr = std::dynamic_pointer_cast<FilledJoinStep>(query_plan_step))
        {
            return std::make_shared<FilledJoinStep>(
                fill_join_step_ptr->input_streams[0], fill_join_step_ptr->join, fill_join_step_ptr->max_block_size);
        }
        if (auto intersect_or_except_step_ptr = std::dynamic_pointer_cast<IntersectOrExceptStep>(query_plan_step))
        {
            return std::make_shared<IntersectOrExceptStep>(
                intersect_or_except_step_ptr->input_streams,
                intersect_or_except_step_ptr->current_operator,
                intersect_or_except_step_ptr->max_threads);
        }
        if (auto creating_set_step = std::dynamic_pointer_cast<CreatingSetStep>(query_plan_step))
        {
            auto set_and_key = std::make_shared<SetAndKey>();
            set_and_key->key = creating_set_step->set_and_key->key;
            set_and_key->set = creating_set_step->set_and_key->set;

            return std::make_shared<CreatingSetStep>(
                creating_set_step->input_streams[0],
                set_and_key,
                creating_set_step->external_table,
                creating_set_step->network_transfer_limits,
                creating_set_step->context);
        }
        if (auto creating_sets_step = std::dynamic_pointer_cast<CreatingSetsStep>(query_plan_step))
            return std::make_shared<CreatingSetsStep>(creating_sets_step->getInputStreams());
        if (auto assign_uniqueid_step_ptr = std::dynamic_pointer_cast<AssignUniqueIdStepExt>(query_plan_step))
            return assign_uniqueid_step_ptr->copy(nullptr);
        if (auto expand_step_ptr = std::dynamic_pointer_cast<ExpandStepExt>(query_plan_step))
            return expand_step_ptr->copy(nullptr);
        if (auto mark_distinct_step_ptr = std::dynamic_pointer_cast<MarkDistinctStepExt>(query_plan_step))
            return mark_distinct_step_ptr->copy(nullptr);
        if (auto setting_quota_and_limits_step_ptr = std::dynamic_pointer_cast<SettingQuotaAndLimitsStepExt>(query_plan_step))
            return setting_quota_and_limits_step_ptr->copy(nullptr);
        if (auto topn_filtering_step_ptr = std::dynamic_pointer_cast<TopNFilteringStepExt>(query_plan_step))
            return topn_filtering_step_ptr->copy(nullptr);
        if (auto exchange_step_ptr = std::dynamic_pointer_cast<ExchangeStepExt>(query_plan_step))
        {
            return exchange_step_ptr->copy(context);
        }
        if (auto local_exchange_step_ptr = std::dynamic_pointer_cast<LocalExchangeStepExt>(query_plan_step))
        {
            return local_exchange_step_ptr->copy(context);
        }
        if (auto remote_exchange_source_step_ptr = std::dynamic_pointer_cast<RemoteExchangeSourceStepExt>(query_plan_step))
        {
            return remote_exchange_source_step_ptr->copy(context);
        }
        if (auto filter_step = std::dynamic_pointer_cast<FilterStepExt>(query_plan_step))
        {
            return filter_step->copy(context);
            if (auto aggregating_step = std::dynamic_pointer_cast<AggregatingStep>(query_plan_step))
            {
                return std::make_shared<AggregatingStep>(
                    aggregating_step->input_streams[0],
                    aggregating_step->params,
                    aggregating_step->grouping_sets_params,
                    aggregating_step->final,
                    aggregating_step->max_block_size,
                    aggregating_step->aggregation_in_order_max_block_bytes,
                    aggregating_step->merge_threads,
                    aggregating_step->temporary_data_merge_threads,
                    aggregating_step->storage_has_evenly_distributed_read,
                    aggregating_step->group_by_use_nulls,
                    aggregating_step->sort_description_for_merging,
                    aggregating_step->group_by_sort_description,
                    aggregating_step->should_produce_results_in_order_of_bucket_number,
                    aggregating_step->memory_bound_merging_of_aggregation_results_enabled,
                    aggregating_step->explicit_sorting_required_for_aggregation_in_order);
            }
            if (auto merging_aggregated_step = std::dynamic_pointer_cast<MergingAggregatedStep>(query_plan_step))
            {
                return std::make_shared<MergingAggregatedStep>(
                    merging_aggregated_step->input_streams[0],
                    merging_aggregated_step->params,
                    merging_aggregated_step->final,
                    merging_aggregated_step->memory_efficient_aggregation,
                    merging_aggregated_step->max_threads,
                    merging_aggregated_step->memory_efficient_merge_threads,
                    merging_aggregated_step->should_produce_results_in_order_of_bucket_number,
                    merging_aggregated_step->max_block_size,
                    merging_aggregated_step->memory_bound_merging_max_block_bytes,
                    merging_aggregated_step->group_by_sort_description,
                    merging_aggregated_step->memory_bound_merging_of_aggregation_results_enabled);
            }
        return nullptr;
    }
};

}