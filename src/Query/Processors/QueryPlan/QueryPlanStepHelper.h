#pragma once

#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/RollupStep.h>
#include <Query/Processors/QueryPlan/BufferStepExt.h>
#include <Query/Processors/QueryPlan/EnforceSingleRowStepExt.h>
#include <Query/Processors/QueryPlan/ExplainAnalyzeStepExt.h>
#include <Query/Processors/QueryPlan/PartitionTopNStepExt.h>
#include <Query/Processors/QueryPlan/ProjectionStepExt.h>

//#include <Processors/QueryPlan/JoinStep.h>
//#include <Processors/QueryPlan/MultiJoinStep.h>

namespace DB
{
using QueryPlanStepShardPtr = std::shared_ptr<IQueryPlanStep>;


#define APPLY_QUERY_PLAN_STEP_TYPES(M) \
    M(CubeStep) \
    M(ExtremesStep) \
    M(RollupStep) \
    M(OffsetStep) \
    M(BufferStepExt) \
    M(EnforceSingleRowStepExt) \
    M(ExplainAnalyzeStepExt) \
    M(PartitionTopNStepExt) \
    M(ProjectionStepExt)

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
        if (auto step_ptr = std::dynamic_pointer_cast<OffsetStep>(query_plan_step))
        {
            return std::make_shared<OffsetStep>(step_ptr->input_streams[0], step_ptr->offset);
        }
        else if (auto join_step_ptr = std::dynamic_pointer_cast<JoinStep>(query_plan_step))
        {
            //TODO: need to add JoinStep copy logic
            return nullptr;
        }
        else if (auto step_ptr = std::dynamic_pointer_cast<BufferStepExt>(query_plan_step))
        {
            return std::make_shared<BufferStepExt>(step_ptr->input_streams[0]);
        }
        else if (auto step_ptr = std::dynamic_pointer_cast<EnforceSingleRowStepExt>(query_plan_step))
        {
            return std::make_unique<EnforceSingleRowStepExt>(step_ptr->input_streams[0]);
        }
        else if (auto step_ptr = std::dynamic_pointer_cast<ExplainAnalyzeStepExt>(query_plan_step))
        {
            return std::make_shared<ExplainAnalyzeStepExt>(
                step_ptr->input_streams[0],
                step_ptr->getOutputName(),
                step_ptr->kind,
                step_ptr->context,
                step_ptr->query_plan_ptr,
                step_ptr->settings);
        }
        else if (auto step_ptr = std::dynamic_pointer_cast<PartitionTopNStepExt>(query_plan_step))
        {
            return std::make_shared<PartitionTopNStepExt>(
                step_ptr->input_streams[0], step_ptr->partition, step_ptr->order_by, step_ptr->limit, step_ptr->model);
        }
        else if (auto step_ptr = std::dynamic_pointer_cast<ProjectionStepExt>(query_plan_step))
        {
            return std::make_shared<ProjectionStepExt>(
                step_ptr->input_streams[0],
                step_ptr->assignments.copy(),
                step_ptr->name_to_type,
                step_ptr->final_project,
                step_ptr->index_project);
        }

        return nullptr;
    }
};

}
