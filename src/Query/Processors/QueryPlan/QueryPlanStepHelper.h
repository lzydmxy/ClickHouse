#pragma once

#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
//#include <Processors/QueryPlan/JoinStep.h>
//#include <Processors/QueryPlan/MultiJoinStep.h>

namespace DB
{
using QueryPlanStep = std::shared_ptr<IQueryPlanStep>;


#define APPLY_QUERY_PLAN_STEP_TYPES(M) \
    M(ExtremesStep) \
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

inline QueryPlanStepType getQueryPlanStepType(const QueryPlanStep & query_plan_step)
{
    APPLY_QUERY_PLAN_STEP_TYPES(CHECK_AND_RETURN_QUERY_PLAN_STEP_TYPE)
    return QueryPlanStepType::UNDEFINED;
}
#undef CHECK_AND_RETURN_QUERY_PLAN_STEP_TYPE


inline bool isPhysicalQueryPlanStep(const QueryPlanStep & query_plan_step)
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

inline bool isLogicalQueryPlanStep(const QueryPlanStep & query_plan_step)
{
    return !isPhysicalQueryPlanStep(query_plan_step);
}

}
