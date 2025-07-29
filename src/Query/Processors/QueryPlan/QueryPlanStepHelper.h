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
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/RollupStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Processors/QueryPlan/ReadNothingStep.h>

#include <Query/Processors/QueryPlan/AggregatingStepExt.h>
#include <Query/Processors/QueryPlan/AnyStepExt.h>
#include <Query/Processors/QueryPlan/ApplyStepExt.h>
#include <Query/Processors/QueryPlan/AssignUniqueIdStepExt.h>
#include <Query/Processors/QueryPlan/BufferStepExt.h>
#include <Query/Processors/QueryPlan/CTERefStepExt.h>
#include <Query/Processors/QueryPlan/DistinctStepExt.h>
#include <Query/Processors/QueryPlan/EnforceSingleRowStepExt.h>
#include <Query/Processors/QueryPlan/ExceptStepExt.h>
#include <Query/Processors/QueryPlan/ExchangeStepExt.h>
#include <Query/Processors/QueryPlan/ExpandStepExt.h>
#include <Query/Processors/QueryPlan/ExplainAnalyzeStepExt.h>
#include <Query/Processors/QueryPlan/FilterStepExt.h>
#include <Query/Processors/QueryPlan/FinalSampleStepExt.h>
#include <Query/Processors/QueryPlan/IntermediateResultCacheStepExt.h>
#include <Query/Processors/QueryPlan/IntersectStepExt.h>
#include <Query/Processors/QueryPlan/JoinStepExt.h>
#include <Query/Processors/QueryPlan/LocalExchangeStepExt.h>
#include <Query/Processors/QueryPlan/MarkDistinctStepExt.h>
#include <Query/Processors/QueryPlan/MultiJoinStepExt.h>
#include <Query/Processors/QueryPlan/PartitionTopNStepExt.h>
#include <Query/Processors/QueryPlan/PlanSegmentSourceStepExt.h>
#include <Query/Processors/QueryPlan/ProjectionStepExt.h>
#include <Query/Processors/QueryPlan/RemoteExchangeSourceStepExt.h>
#include <Query/Processors/QueryPlan/SettingQuotaAndLimitsStepExt.h>
#include <Query/Processors/QueryPlan/TableScanStepExt.h>
#include <Query/Processors/QueryPlan/TopNFilteringStepExt.h>
#include <Query/Processors/QueryPlan/TotalsHavingStepExt.h>
#include <Query/Processors/QueryPlan/UnionStepExt.h>
#include <Query/Processors/QueryPlan/ValuesStepExt.h>
#include <Query/Processors/QueryPlan/LimitStepExt.h>
#include <Query/Processors/QueryPlan/ReadStorageRowCountStepExt.h>
#include <Query/Processors/QueryPlan/MergingAggregatedStepExt.h>
#include <Query/Processors/QueryPlan/MergeSortingStepExt.h>
#include <Query/Processors/QueryPlan/MergingSortedStepExt.h>
#include <Query/Processors/QueryPlan/PartialSortingStepExt.h>
#include <Query/Processors/QueryPlan/SortingStepExt.h>
#include <Query/Processors/QueryPlan/FinishSortingStepExt.h>
#include <Query/ProtosHelper/PlanSerDerHelper.h>

namespace DB
{

// protobuf's types and names for StepExt with proto
#define APPLY_PROTOBUF_STEP_TYPES_AND_NAMES_FOR_EXT(M) \
    M(AggregatingStepExt, aggregating_step_ext) \
    M(ApplyStepExt, apply_step_ext) \
    M(AssignUniqueIdStepExt, assign_unique_id_step_ext) \
    M(BufferStepExt, buffer_step_ext) \
    M(CTERefStepExt, c_t_e_ref_step_ext) \
    M(DistinctStepExt, distinct_step_ext) \
    M(EnforceSingleRowStepExt, enforce_single_row_step_ext) \
    M(ExceptStepExt, except_step_ext) \
    M(ExchangeStepExt, exchange_step_ext) \
    M(ExpandStepExt, expand_step_ext) \
    M(ExplainAnalyzeStepExt, explain_analyze_step_ext) \
    M(FilterStepExt, filter_step_ext) \
    M(FinalSampleStepExt, final_sample_step_ext) \
    M(IntermediateResultCacheStepExt, intermediate_result_cache_step_ext) \
    M(IntersectStepExt, intersect_step_ext) \
    M(JoinStepExt, join_step_ext) \
    M(LocalExchangeStepExt, local_exchange_step_ext) \
    M(MarkDistinctStepExt, mark_distinct_step_ext) \
    M(MultiJoinStepExt, multi_join_step_ext) \
    M(MergingAggregatedStepExt, merging_aggregated_step_ext) \
    M(PartitionTopNStepExt, partition_top_n_step_ext) \
    M(ProjectionStepExt, projection_step_ext) \
    M(RemoteExchangeSourceStepExt, remote_exchange_source_step_ext) \
    M(TableScanStepExt, table_scan_step_ext) \
    M(TopNFilteringStepExt, top_n_filtering_step_ext) \
    M(TotalsHavingStepExt, totals_having_step_ext) \
    M(UnionStepExt, union_step_ext) \
    M(ValuesStepExt, values_step_ext) \
    M(LimitStepExt, limit_step_ext) \
    M(ReadStorageRowCountStepExt, read_storage_row_count_step_ext) \
    M(MergeSortingStepExt, merge_sorting_step_ext) \
    M(MergingSortedStepExt, merging_sorted_step_ext) \
    M(PartialSortingStepExt, partial_sorting_step_ext) \
    M(SortingStepExt, sorting_step_ext) \
    M(FinishSortingStepExt, finish_sorting_step_ext)

// protobuf's types and names for Step with proto
#define APPLY_PROTOBUF_STEP_TYPES_AND_NAMES(M) \
    APPLY_PROTOBUF_STEP_TYPES_AND_NAMES_FOR_EXT(M) \
    M(ArrayJoinStep, array_join_step) \
    M(ExtremesStep, extremes_step) \
    M(FillingStep, filling_step) \
    M(IntersectOrExceptStep, intersect_or_except_step) \
    M(LimitByStep, limit_by_step) \
    M(OffsetStep, offset_step) \
    M(ReadNothingStep, read_nothing_step) \
    M(WindowStep, window_step) \

// types for StepExt without proto
#define APPLY_NOPROTOBUF_STEP_TYPES_FOR_EXT(M) \
    M(PlanSegmentSourceStepExt) \
    M(SettingQuotaAndLimitsStepExt) \

// types for Step without proto
#define APPLY_NOPROTOBUF_STEP_TYPES(M) \
    APPLY_NOPROTOBUF_STEP_TYPES_FOR_EXT(M) \
    M(AggregatingProjectionStep) \
    M(ReadFromMergeTree) \
    M(ReadFromPreparedSource) \
    M(CreatingSetStep) \
    M(CreatingSetsStep) \
    M(CubeStep) \
    M(ExpressionStep) \
    M(FilledJoinStep) \
    M(ReadFromStorageStep) \
    M(RollupStep) \

// macro helpers to convert MM(x, y) to M(x)
#define IMPL_TUPLE_TO_FIRST(_x, _y) (_x)
// extract types from StepExt and Step with proto
#define IMPL_PROTOBUF_STEP_TYPES_FOR_EXT APPLY_PROTOBUF_STEP_TYPES_AND_NAMES_FOR_EXT(IMPL_TUPLE_TO_FIRST)
#define IMPL_PROTOBUF_STEP_TYPES APPLY_PROTOBUF_STEP_TYPES_AND_NAMES(IMPL_TUPLE_TO_FIRST)

// apply function
#define IMPL_MACRO_FUNCTION_APPLY(_r, _data, _elem) _data(_elem)
// apply function for elements in seq
#define APPLY_PROTOBUF_STEP_TYPES_FOR_EXT(M) BOOST_PP_SEQ_FOR_EACH(IMPL_MACRO_FUNCTION_APPLY, M, IMPL_PROTOBUF_STEP_TYPES_FOR_EXT)
#define APPLY_PROTOBUF_STEP_TYPES(M) BOOST_PP_SEQ_FOR_EACH(IMPL_MACRO_FUNCTION_APPLY, M, IMPL_PROTOBUF_STEP_TYPES)

// all types for StepExt
#define APPLY_ALL_STEP_TYPES_FOR_EXT(M) \
    APPLY_PROTOBUF_STEP_TYPES_FOR_EXT(M) \
    APPLY_NOPROTOBUF_STEP_TYPES_FOR_EXT(M)
// all types for Step
#define APPLY_ALL_STEP_TYPES(M) \
    APPLY_PROTOBUF_STEP_TYPES(M) \
    APPLY_NOPROTOBUF_STEP_TYPES(M)

#define ENUM_QUERY_PLAN_STEP_TYPE(ITEM) ITEM,
enum class QueryPlanStepType : UInt8
{
    AnyStepExt = 0,
    // change this when order is changed to avoid conflicts
    StepBegin = 100,
    APPLY_ALL_STEP_TYPES(ENUM_QUERY_PLAN_STEP_TYPE) UNDEFINED,
    Tree,
};
#undef ENUM_QUERY_PLAN_STEP_TYPE

inline String toString(QueryPlanStepType type)
{
    switch (type)
    {
#define ENUM_QUERY_PLAN_STEP_TYPE(ITEM) \
    case QueryPlanStepType::ITEM: \
        return #ITEM;
    APPLY_ALL_STEP_TYPES(ENUM_QUERY_PLAN_STEP_TYPE)
    ENUM_QUERY_PLAN_STEP_TYPE(AnyStepExt)
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

inline QueryPlanStepType getQueryPlanStepType(const QueryPlanStepPtr & query_plan_step)
{
    APPLY_ALL_STEP_TYPES(CHECK_AND_RETURN_QUERY_PLAN_STEP_TYPE)
    CHECK_AND_RETURN_QUERY_PLAN_STEP_TYPE(AnyStepExt)
    return QueryPlanStepType::UNDEFINED;
}
#undef CHECK_AND_RETURN_QUERY_PLAN_STEP_TYPE

#define CHECK_AND_RETURN_QUERY_PLAN_STEP_TYPE_REF(type) \
    if (typeInfo == typeid(type)) \
    { \
        return QueryPlanStepType::type; \
    }

inline QueryPlanStepType getQueryPlanStepType(const IQueryPlanStep & query_plan_step)
{
    const std::type_info & typeInfo = typeid(query_plan_step);
    APPLY_ALL_STEP_TYPES(CHECK_AND_RETURN_QUERY_PLAN_STEP_TYPE_REF)
    CHECK_AND_RETURN_QUERY_PLAN_STEP_TYPE_REF(AnyStepExt)
    return QueryPlanStepType::UNDEFINED;
}
#undef CHECK_AND_RETURN_QUERY_PLAN_STEP_TYPE_REF


using DataStreamSortScope = DataStream::SortScope;
ENUM_TO_PROTO_CONVERTER(
    DataStreamSortScope,
    Protos::DataStream::SortScope,
    (None),
    (Chunk),
    (Stream),
    (Global)
);

using WindowFrameType = WindowFrame::FrameType;
ENUM_TO_PROTO_CONVERTER(
    WindowFrameType, // enum name
    Protos::WindowFrame::FrameType, // proto enum message
    (ROWS),
    (GROUPS),
    (RANGE));

using WindowFrameBoundaryType = WindowFrame::BoundaryType;
ENUM_TO_PROTO_CONVERTER(
    WindowFrameBoundaryType, // enum name
    Protos::WindowFrame::BoundaryType, // proto enum message
    (Unbounded),
    (Current),
    (Offset));

using ASTSelectIntersectExceptQueryOperator = ASTSelectIntersectExceptQuery::Operator;
ENUM_TO_PROTO_CONVERTER(
    ASTSelectIntersectExceptQueryOperator, // enum name
    Protos::IntersectExceptOperator, // proto enum message
    (UNKNOWN),
    (EXCEPT_ALL),
    (EXCEPT_DISTINCT),
    (INTERSECT_ALL),
    (INTERSECT_DISTINCT));

/// diff bc has ExcludeType

using FieldTypeWhich = Field::Types::Which;
ENUM_TO_PROTO_CONVERTER(
    FieldTypeWhich, // enum name
    Protos::Field::FieldType, // proto enum message
    (Null, 0),
    (UInt64, 1),
    (Int64, 2),
    (Float64, 3),
    (UInt128, 4),
    (Int128, 5),

    (String, 16),
    (Array, 17),
    (Tuple, 18),
    (Decimal32, 19),
    (Decimal64, 20),
    (Decimal128, 21),
    (AggregateFunctionState, 22),
    (Decimal256, 23),
    (UInt256, 24),
    (Int256, 25),
    (Map, 26),
    (UUID, 27),
    (Bool, 28),
    (Object, 29),
    (IPv4, 30),
    (IPv6, 31),
    (CustomType, 32),
    (SketchBinary, 100));

class QueryPlanStepHelper
{
public:
    QueryPlanStepHelper() = default;
    ~QueryPlanStepHelper() = default;

    static ActionsDAGPtr createFilterExpressionActions(ContextPtr context, const ASTPtr & filter, const Block & header);
    static ActionsDAGPtr createExpressionActions(ContextPtr context, const NamesAndTypesList & source, const Names & output, const ASTPtr & ast, bool add_project = true);
    static ActionsDAGPtr createExpressionActions(ContextPtr context, const NamesAndTypesList & source, const NamesWithAliases & output, const ASTPtr & ast, bool add_project = true);
    static void projection(QueryPipelineBuilder & pipeline, const Block & target, const BuildQueryPipelineSettings & settings);
    static bool isLogicalQueryPlanStep(const QueryPlanStepPtr & query_plan_step) { return !isPhysicalQueryPlanStep(query_plan_step); }

    static bool isPhysicalQueryPlanStep(const QueryPlanStepPtr & query_plan_step)
    {
        if (auto join_step_ext = std::dynamic_pointer_cast<JoinStepExt>(query_plan_step))
            return join_step_ext->getDistributionType() != DistributionType::UNKNOWN;
        if (auto casted_query_plan_step = std::dynamic_pointer_cast<MultiJoinStepExt>(query_plan_step))
            return false;

        return true;
    }

    static bool isQueryPlanStepEqual(const IQueryPlanStep & lhs, const IQueryPlanStep & rhs)
    {
        return isPlanStepEqual(lhs, rhs);
    }

    static QueryPlanStepPtr copyQueryPlanStep(const QueryPlanStepPtr & query_plan_step, ContextPtr context);

    // template <typename StepType, typename ProtoType>
    // static void toProto(const StepType & step, ProtoType & proto, bool for_hash_equals = false)
    // {
    //     step.toProto(proto, for_hash_equals);
    // }

#define TO_PROTO_DEF(TYPE, VAR_NAME) \
    static void toProto(const TYPE & step, Protos::TYPE & proto, bool for_hash_equals = false);

    APPLY_PROTOBUF_STEP_TYPES_AND_NAMES(TO_PROTO_DEF)
#undef TO_PROTO_DEF


    static void toProto(const IQueryPlanStep & query_plan_step, Protos::QueryPlanStep & proto, bool for_hash_equals = false);

    template <typename ProtoType>
    static void toProto(const IQueryPlanStep & query_plan_step, ProtoType & proto, bool for_hash_equals = false)
    {
        switch (getQueryPlanStepType(query_plan_step))
        {
            // 1. StepExt with proto uses macros to execute toProto, see PROTOBUF_STEP_TYPES_AND_NAMES_FOR_EXT
#define CASE_DEF(TYPE, VAR_NAME) \
case QueryPlanStepType::TYPE: { \
const auto & step = dynamic_cast<const TYPE &>(query_plan_step); \
toProto(step, proto, for_hash_equals); \
break; \
}

            APPLY_PROTOBUF_STEP_TYPES_AND_NAMES(CASE_DEF)
    #undef CASE_DEF
            default: {
                throw Exception(ErrorCodes::PROTOBUF_BAD_CAST, "not implemented step: {}", static_cast<int>(getQueryPlanStepType(query_plan_step)));
            }
        }
    }


#define FROM_PROTO_DEF(TYPE, VAR_NAME) \
    static QueryPlanStepPtr fromProto(const Protos::TYPE & proto, ContextPtr context);

    APPLY_PROTOBUF_STEP_TYPES_AND_NAMES(FROM_PROTO_DEF)
#undef FROM_PROTO_DEF

    static QueryPlanStepPtr fromProto(const Protos::QueryPlanStep & proto, ContextPtr context);

    static const Names & getLimitByStepColumns(const LimitByStep & limit) { return limit.columns; }
    static size_t getLimitByStepGroupLength(const LimitByStep & limit) { return limit.group_length; }
    static size_t getLimitByStepGroupOffset(const LimitByStep & limit) { return limit.group_offset; }

    static size_t getOffsetStepOffset(const OffsetStep & offset) {return offset.offset;}

    static const std::vector<WindowFunctionDescription> & getWindowStepFunctions(const WindowStep & window) {return window.window_functions;}
    static bool getWindowStepStreamsFanOut(const WindowStep & window) {return window.streams_fan_out;}
    static const WindowDescription & getWindowStepWindow(const WindowStep & window) {return window.window_description;}

    static const SortDescription & getFillingStepFillDescription(const FillingStep & filling_step) {return filling_step.fill_description;}
    static bool getFillingStepUseWithFillBySortingPrefix(const FillingStep & filling_step) {return filling_step.use_with_fill_by_sorting_prefix;}

    static String getIntersectOrExceptStepOperatorStr(const IntersectOrExceptStep & intersect_or_except_step)
    {
        const auto & name = ASTSelectIntersectExceptQueryOperatorConverter::toString(intersect_or_except_step.current_operator);
        if (name.empty())
            return "UNKNOWN";
        return name;
    }


    static const ASTSelectIntersectExceptQuery::Operator & getIntersectOrExceptStepOperator(const IntersectOrExceptStep & intersect_or_except) {return intersect_or_except.current_operator;}
    static size_t getIntersectOrExceptStepMaxThreads(const IntersectOrExceptStep & intersect_or_except) {return intersect_or_except.max_threads;}
};

}
