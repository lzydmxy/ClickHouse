#pragma once

#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/RollupStep.h>

#include <Query/Processors/QueryPlan/ValuesStepExt.h>

//#include <Processors/QueryPlan/JoinStep.h>
//#include <Processors/QueryPlan/MultiJoinStep.h>

namespace DB
{
using QueryPlanStepShardPtr = std::shared_ptr<IQueryPlanStep>;


#define APPLY_QUERY_PLAN_STEP_TYPES(M) \
    M(ArrayJoinStep) \
    M(CubeStep) \
    M(DistinctStep) \
    M(ExpressionStep) \
    M(ExtremesStep) \
    M(LimitByStep) \
    M(OffsetStep) \
    M(ReadFromPreparedSource) \
    M(ReadFromStorageStep) \
    M(RollupStep) \
    M(ValuesStepExt)


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
        if (auto step_ptr = std::dynamic_pointer_cast<ArrayJoinStep>(query_plan_step))
        {
            return std::make_shared<ArrayJoinStep>(step_ptr->input_streams[0], step_ptr->array_join);
        }
        else if (auto step_ptr = std::dynamic_pointer_cast<CubeStep>(query_plan_step))
        {
            return std::make_shared<CubeStep>(step_ptr->input_streams[0], step_ptr->params, step_ptr->final, step_ptr->use_nulls);
        }
        else if (auto step_ptr = std::dynamic_pointer_cast<DistinctStep>(query_plan_step))
        {
            //return std::make_shared<DistinctStep>(step_ptr->input_streams[0], step_ptr->set_size_limits, step_ptr->limit_hint, step_ptr->columns, step_ptr->pre_distinct, step_ptr->can_to_agg);
            return nullptr;
        }
        else if (auto step_ptr = std::dynamic_pointer_cast<ExpressionStep>(query_plan_step))
        {
            return std::make_shared<ExpressionStep>(step_ptr->input_streams[0], step_ptr->actions_dag);
        }
        else if (auto step_ptr = std::dynamic_pointer_cast<ExtremesStep>(query_plan_step))
        {
            return std::make_shared<ExtremesStep>(step_ptr->input_streams[0]);
        }
        else if (auto step_ptr = std::dynamic_pointer_cast<LimitByStep>(query_plan_step))
        {
            return std::make_shared<LimitByStep>(step_ptr->input_streams[0], step_ptr->group_length, step_ptr->group_offset, step_ptr->columns);
        }
        else if (auto step_ptr = std::dynamic_pointer_cast<OffsetStep>(query_plan_step))
        {
            return std::make_shared<OffsetStep>(step_ptr->input_streams[0], step_ptr->offset);
        }
        else if (auto step_ptr = std::dynamic_pointer_cast<ReadFromPreparedSource>(query_plan_step))
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ReadFromPreparedSource can not copy");
        }
        else if (auto step_ptr = std::dynamic_pointer_cast<ReadFromStorageStep>(query_plan_step))
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ReadFromStorageStep can not copy");
        }
        else if (auto step_ptr = std::dynamic_pointer_cast<RollupStep>(query_plan_step))
        {
            return std::make_shared<RollupStep>(step_ptr->input_streams[0], step_ptr->params, step_ptr->final, step_ptr->use_nulls);
        }
        else if (auto step_ptr = std::dynamic_pointer_cast<ValuesStepExt>(query_plan_step))
        {
            return std::make_shared<ValuesStepExt>(step_ptr->output_stream->header, step_ptr->fields);
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
