#pragma once

#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/ExtremesStep.h>
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


namespace DB
{
using QueryPlanStepShardPtr = std::shared_ptr<IQueryPlanStep>;


#define APPLY_QUERY_PLAN_STEP_TYPES(M) \
    M(CubeStep) \
    M(ExtremesStep) \
    M(RollupStep) \
    M(OffsetStep) \
    M(JoinStepExt) \
    M(FilledJoinStep) \
    M(MultiJoinStepExt) \
    M(UnionStepExt) \
    M(IntermediateResultCacheStepExt) \
    M(CreatingSetStep) \
    M(CreatingSetsStep) \
    M(IntersectOrExceptStep) \
    M(ApplyStepExt) \
    M(AnyStepExt)

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


        return nullptr;
    }
};

}
