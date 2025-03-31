#pragma once

#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/IntersectOrExceptStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/RollupStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/WindowStep.h>

#include <Query/Processors/QueryPlan/TableScanStepExt.h>
#include <Query/Processors/QueryPlan/AnyStepExt.h>
#include <Query/Processors/QueryPlan/ApplyStepExt.h>
#include <Query/Processors/QueryPlan/AssignUniqueIdStepExt.h>
#include <Query/Processors/QueryPlan/BufferStepExt.h>
#include <Query/Processors/QueryPlan/CTERefStepExt.h>
#include <Query/Processors/QueryPlan/EnforceSingleRowStepExt.h>
#include <Query/Processors/QueryPlan/ExchangeStepExt.h>
#include <Query/Processors/QueryPlan/ExpandStepExt.h>
#include <Query/Processors/QueryPlan/ExplainAnalyzeStepExt.h>
#include <Query/Processors/QueryPlan/FilterStepExt.h>
#include <Query/Processors/QueryPlan/IntermediateResultCacheStepExt.h>
#include <Query/Processors/QueryPlan/JoinStepExt.h>
#include <Query/Processors/QueryPlan/LocalExchangeStepExt.h>
#include <Query/Processors/QueryPlan/MarkDistinctStepExt.h>
#include <Query/Processors/QueryPlan/MultiJoinStepExt.h>
#include <Query/Processors/QueryPlan/PartitionTopNStepExt.h>
#include <Query/Processors/QueryPlan/PlanSegmentSourceStepExt.h>
#include <Query/Processors/QueryPlan/ProjectionStepExt.h>
#include <Query/Processors/QueryPlan/RemoteExchangeSourceStepExt.h>
#include <Query/Processors/QueryPlan/SettingQuotaAndLimitsStepExt.h>
#include <Query/Processors/QueryPlan/TopNFilteringStepExt.h>
#include <Query/Processors/QueryPlan/UnionStepExt.h>
#include <Query/Processors/QueryPlan/ValuesStepExt.h>

namespace DB
{
using QueryPlanStepSharedPtr = std::shared_ptr<IQueryPlanStep>;

class TableScanStepExt;


#define APPLY_QUERY_PLAN_STEP_TYPES(M) \
    M(AggregatingProjectionStep) \
    M(AggregatingStep) \
    M(AnyStepExt) \
    M(ApplyStepExt) \
    M(ArrayJoinStep) \
    M(AssignUniqueIdStepExt) \
    M(BufferStepExt) \
    M(CreatingSetStep) \
    M(CreatingSetsStep) \
    M(CubeStep) \
    M(CTERefStepExt) \
    M(EnforceSingleRowStepExt) \
    M(ExchangeStepExt) \
    M(ExpandStepExt) \
    M(ExplainAnalyzeStepExt) \
    M(ExpressionStep) \
    M(ExtremesStep) \
    M(FilledJoinStep) \
    M(FillingStep) \
    M(FilterStepExt) \
    M(IntermediateResultCacheStepExt) \
    M(IntersectOrExceptStep) \
    M(JoinStepExt) \
    M(LimitByStep) \
    M(LimitStep) \
    M(TableScanStepExt) \
    M(LocalExchangeStepExt) \
    M(MarkDistinctStepExt) \
    M(MergingAggregatedStep) \
    M(MultiJoinStepExt) \
    M(OffsetStep) \
    M(PartitionTopNStepExt) \
    M(PlanSegmentSourceStepExt) \
    M(ProjectionStepExt) \
    M(ReadFromPreparedSource) \
    M(ReadFromStorageStep) \
    M(RemoteExchangeSourceStepExt) \
    M(RollupStep) \
    M(SettingQuotaAndLimitsStepExt) \
    M(SortingStep) \
    M(TopNFilteringStepExt) \
    M(UnionStepExt) \
    M(ValuesStepExt) \
    M(WindowStep)

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

inline QueryPlanStepType getQueryPlanStepType(const QueryPlanStepSharedPtr & query_plan_step)
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

    static ActionsDAGPtr createFilterExpressionActions(ContextPtr context, const ASTPtr & filter, const Block & header);
    static ActionsDAGPtr createExpressionActions(ContextPtr context, const NamesAndTypesList & source, const Names & output, const ASTPtr & ast, bool add_project = true);
    static ActionsDAGPtr createExpressionActions(ContextPtr context, const NamesAndTypesList & source, const NamesWithAliases & output, const ASTPtr & ast, bool add_project = true);
    static void projection(QueryPipelineBuilder & pipeline, const Block & target, const BuildQueryPipelineSettings & settings);
    static bool isLogicalQueryPlanStep(const QueryPlanStepSharedPtr & query_plan_step) { return !isPhysicalQueryPlanStep(query_plan_step); }

    static bool isPhysicalQueryPlanStep(const QueryPlanStepSharedPtr & query_plan_step)
    {
        if (auto join_step_ext = std::dynamic_pointer_cast<JoinStepExt>(query_plan_step))
            return join_step_ext->getDistributionType() != DistributionType::UNKNOWN;
        if (auto casted_query_plan_step = std::dynamic_pointer_cast<MultiJoinStepExt>(query_plan_step))
            return false;

        return true;
    }

    static QueryPlanStepSharedPtr copyQueryPlanStep(const QueryPlanStepSharedPtr & query_plan_step, ContextPtr context)
    {
        if (auto step_ptr = std::dynamic_pointer_cast<OffsetStep>(query_plan_step))
            return std::make_shared<OffsetStep>(step_ptr->input_streams[0], step_ptr->offset);
        else if (auto step_ptr = std::dynamic_pointer_cast<ArrayJoinStep>(query_plan_step))
            return std::make_shared<ArrayJoinStep>(step_ptr->input_streams[0], step_ptr->array_join);
        else if (auto step_ptr = std::dynamic_pointer_cast<CubeStep>(query_plan_step))
            return std::make_shared<CubeStep>(step_ptr->input_streams[0], step_ptr->params, step_ptr->final, step_ptr->use_nulls);
        else if (auto step_ptr = std::dynamic_pointer_cast<ExpressionStep>(query_plan_step))
            return std::make_shared<ExpressionStep>(step_ptr->input_streams[0], step_ptr->actions_dag);
        else if (auto step_ptr = std::dynamic_pointer_cast<ExtremesStep>(query_plan_step))
            return std::make_shared<ExtremesStep>(step_ptr->input_streams[0]);
        else if (auto step_ptr = std::dynamic_pointer_cast<LimitByStep>(query_plan_step))
            return std::make_shared<LimitByStep>(step_ptr->input_streams[0], step_ptr->group_length, step_ptr->group_offset, step_ptr->columns);
        else if (auto step_ptr = std::dynamic_pointer_cast<FilledJoinStep>(query_plan_step))
            return std::make_shared<FilledJoinStep>(step_ptr->input_streams[0], step_ptr->join, step_ptr->max_block_size);
        else if (auto step_ptr = std::dynamic_pointer_cast<ReadFromPreparedSource>(query_plan_step))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ReadFromPreparedSource can not copy");
        else if (auto step_ptr = std::dynamic_pointer_cast<ReadFromStorageStep>(query_plan_step))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ReadFromStorageStep can not copy");
        else if (auto step_ptr = std::dynamic_pointer_cast<RollupStep>(query_plan_step))
            return std::make_shared<RollupStep>(step_ptr->input_streams[0], step_ptr->params, step_ptr->final, step_ptr->use_nulls);
        else if (auto step_ptr = std::dynamic_pointer_cast<IntersectOrExceptStep>(query_plan_step))
            return std::make_shared<IntersectOrExceptStep>(step_ptr->input_streams, step_ptr->current_operator, step_ptr->max_threads);
        else if (auto step_ptr = std::dynamic_pointer_cast<CreatingSetStep>(query_plan_step))
        {
            auto set_and_key = std::make_shared<SetAndKey>();
            set_and_key->key = step_ptr->set_and_key->key;
            set_and_key->set = step_ptr->set_and_key->set;

            return std::make_shared<CreatingSetStep>(
                step_ptr->input_streams[0],
                set_and_key,
                step_ptr->external_table,
                step_ptr->network_transfer_limits,
                step_ptr->context);
        }
        else if (auto step_ptr = std::dynamic_pointer_cast<AggregatingStep>(query_plan_step))
        {
                return std::make_shared<AggregatingStep>(
                    step_ptr->input_streams[0],
                    step_ptr->params,
                    step_ptr->grouping_sets_params,
                    step_ptr->final,
                    step_ptr->max_block_size,
                    step_ptr->aggregation_in_order_max_block_bytes,
                    step_ptr->merge_threads,
                    step_ptr->temporary_data_merge_threads,
                    step_ptr->storage_has_evenly_distributed_read,
                    step_ptr->group_by_use_nulls,
                    step_ptr->sort_description_for_merging,
                    step_ptr->group_by_sort_description,
                    step_ptr->should_produce_results_in_order_of_bucket_number,
                    step_ptr->memory_bound_merging_of_aggregation_results_enabled,
                    step_ptr->explicit_sorting_required_for_aggregation_in_order);
        }
        else if (auto step_ptr = std::dynamic_pointer_cast<MergingAggregatedStep>(query_plan_step))
        {
                return std::make_shared<MergingAggregatedStep>(
                    step_ptr->input_streams[0],
                    step_ptr->params,
                    step_ptr->final,
                    step_ptr->memory_efficient_aggregation,
                    step_ptr->max_threads,
                    step_ptr->memory_efficient_merge_threads,
                    step_ptr->should_produce_results_in_order_of_bucket_number,
                    step_ptr->max_block_size,
                    step_ptr->memory_bound_merging_max_block_bytes,
                    step_ptr->group_by_sort_description,
                    step_ptr->memory_bound_merging_of_aggregation_results_enabled);
        }
        else if (auto window_step = std::dynamic_pointer_cast<WindowStep>(query_plan_step))
        {
            return std::make_shared<WindowStep>(
                window_step->input_streams[0],
                window_step->window_description, /// TODO deep copy
                window_step->window_functions, /// TODO deep copy
                window_step->streams_fan_out);
        }
        else if (auto sorting_step = std::dynamic_pointer_cast<SortingStep>(query_plan_step))
        {
            switch (sorting_step->getType())
            {
                case SortingStep::Type::FinishSorting:
                    return std::make_shared<SortingStep>(
                        sorting_step->input_streams[0],
                        sorting_step->prefix_description,
                        sorting_step->result_description,
                        sorting_step->sort_settings.max_block_size,
                        sorting_step->limit);
                case SortingStep::Type::Full:
                    if (!sorting_step->partition_by_description.empty())
                    {
                        return std::make_shared<SortingStep>(
                            sorting_step->input_streams[0],
                            sorting_step->result_description,
                            sorting_step->partition_by_description,
                            sorting_step->limit,
                            sorting_step->sort_settings,
                            sorting_step->optimize_sorting_by_input_stream_properties);
                    }
                    else
                    {
                        return std::make_shared<SortingStep>(
                            sorting_step->input_streams[0],
                            sorting_step->result_description,
                            sorting_step->limit,
                            sorting_step->sort_settings,
                            sorting_step->optimize_sorting_by_input_stream_properties);
                    }
                case SortingStep::Type::MergingSorted:
                    return std::make_shared<SortingStep>(
                            sorting_step->input_streams[0],
                            sorting_step->result_description,
                            sorting_step->sort_settings.max_block_size,
                            sorting_step->always_read_till_end);
            }
        }
        else if (auto filling_step = std::dynamic_pointer_cast<FillingStep>(query_plan_step))
        {
            return std::make_shared<FillingStep>(
                filling_step->input_streams[0],
                filling_step->sort_description,
                filling_step->fill_description,
                filling_step->interpolate_description, /// TODO deep copy
                filling_step->use_with_fill_by_sorting_prefix);
        }
        else if (auto aggregating_projection_step = std::dynamic_pointer_cast<AggregatingProjectionStep>(query_plan_step))
        {
            return std::make_shared<AggregatingProjectionStep>(
                aggregating_projection_step->input_streams,
                aggregating_projection_step->params,
                aggregating_projection_step->final,
                aggregating_projection_step->merge_threads,
                aggregating_projection_step->temporary_data_merge_threads);
        }
        else if (auto step_ptr = std::dynamic_pointer_cast<CreatingSetsStep>(query_plan_step))
            return std::make_shared<CreatingSetsStep>(step_ptr->getInputStreams());

        // steps end by Ext use macroc to copy
        #define CHECK_AND_COPY_QUERY_PLAN_STEP_TYPE_EXT(type) \
        if (auto step_ptr = std::dynamic_pointer_cast<type>(query_plan_step)) \
        { \
            return step_ptr->copy(context); \
        }

        #define APPLY_QUERY_PLAN_STEP_TYPES_EXT(M) \
        M(AnyStepExt) \
        M(ApplyStepExt) \
        M(AssignUniqueIdStepExt) \
        M(BufferStepExt) \
        M(CTERefStepExt) \
        M(EnforceSingleRowStepExt) \
        M(ExchangeStepExt) \
        M(ExpandStepExt) \
        M(ExplainAnalyzeStepExt) \
        M(FilterStepExt) \
        M(IntermediateResultCacheStepExt) \
        M(JoinStepExt) \
        M(LocalExchangeStepExt) \
        M(MarkDistinctStepExt) \
        M(MultiJoinStepExt) \
        M(PartitionTopNStepExt) \
        M(PlanSegmentSourceStepExt) \
        M(ProjectionStepExt) \
        M(RemoteExchangeSourceStepExt) \
        M(SettingQuotaAndLimitsStepExt) \
        M(TopNFilteringStepExt) \
        M(UnionStepExt) \
        M(ValuesStepExt)

        APPLY_QUERY_PLAN_STEP_TYPES_EXT(CHECK_AND_COPY_QUERY_PLAN_STEP_TYPE_EXT)
        #undef CHECK_AND_COPY_QUERY_PLAN_STEP_TYPE_EXT
        #undef APPLY_QUERY_PLAN_STEP_TYPES_EXT

        return nullptr;
    }
};

}
