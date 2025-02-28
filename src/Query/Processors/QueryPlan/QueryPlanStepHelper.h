#pragma once

#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/RollupStep.h>

//#include <Processors/QueryPlan/JoinStep.h>
//#include <Processors/QueryPlan/MultiJoinStep.h>
#include <Query/Processors/QueryPlan/AssignUniqueIdStepExt.h>
#include <Query/Processors/QueryPlan/ExpandStepExt.h>
#include <Query/Processors/QueryPlan/MarkDistinctStepExt.h>
#include <Query/Processors/QueryPlan/SettingQuotaAndLimitsStepExt.h>
#include <Query/Processors/QueryPlan/TopNFilteringStepExt.h>

namespace DB
{
using QueryPlanStepShardPtr = std::shared_ptr<IQueryPlanStep>;


#define APPLY_QUERY_PLAN_STEP_TYPES(M) \
    M(CubeStep) \
    M(ExtremesStep) \
    M(RollupStep) \
    M(OffsetStep) \
    M(LimitStep) \
    M(AssignUniqueIdStepExt) \
    M(ExpandStepExt) \
    M(MarkDistinctStepExt) \
    M(SettingQuotaAndLimitsStepExt) \
    M(TopNFilteringStepExt)

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
    // TODO: FIXME type
    // APPLY_QUERY_PLAN_STEP_TYPES(CHECK_AND_RETURN_QUERY_PLAN_STEP_TYPE)
    return QueryPlanStepType::UNDEFINED;
}
#undef CHECK_AND_RETURN_QUERY_PLAN_STEP_TYPE


class QueryPlanStepHelper
{
public:
    QueryPlanStepHelper() = default;
    ~QueryPlanStepHelper() = default;

    static bool isLogicalQueryPlanStep(const QueryPlanStepShardPtr & query_plan_step)
    {
        return !isPhysicalQueryPlanStep(query_plan_step);
    }

    static bool isPhysicalQueryPlanStep(const QueryPlanStepShardPtr & query_plan_step)
    {
        /* 
        /// TODO: need to  attribute distribution_type to JoinStep
        if (auto casted_query_plan_step = std::dynamic_pointer_cast<JoinStep>(query_plan_step))
        {
            return casted_query_plan_step->distribution_type != DistributionType::UNKNOWN;
        }
        /// TODO: need to add MultiJoinStep.h
        else if (auto casted_query_plan_step = std::dynamic_pointer_cast<MultiJoinStep>(query_plan_step))
        {
            return false;
        }
        */

        return true;
    }

    static QueryPlanStepShardPtr copyQueryPlanStep(const QueryPlanStepShardPtr & query_plan_step)
    {
        if (auto step_ptr = std::dynamic_pointer_cast<OffsetStep>(query_plan_step))
        {
            return std::make_shared<OffsetStep>(step_ptr->input_streams[0], step_ptr->offset);
        }
        else if (auto join_step_ptr = std::dynamic_pointer_cast<JoinStep>(query_plan_step))
        {
            //TODO: need to add JoinStep copy logic
            return nullptr;
        }

        return nullptr;
    }
};

}
