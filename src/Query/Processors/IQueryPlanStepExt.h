#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{
// this must match protobuf message QueryPlanStep
// in src/Protos/plan_node.proto
// where Step/_step postfix is attached.

// Use for Optimizer
#define APPLY_STEP_PROTOBUF_TYPES_AND_NAMES(MM) \
    MM(Aggregating, aggregating) \
    MM(ArrayJoin, array_join) \
    MM(AssignUniqueId, assign_unique_id) \
    MM(CTERef, c_t_e_ref) \
    MM(Distinct, distinct) \
    MM(EnforceSingleRow, enforce_single_row) \
    MM(Except, except) \
    MM(Exchange, exchange) \
    MM(Extremes, extremes) \
    MM(Filling, filling) \
    MM(Filter, filter) \
    MM(Intersect, intersect) \
    MM(Join, join) \
    MM(LimitBy, limit_by) \
    MM(Limit, limit) \
    MM(MarkDistinct, mark_distinct) \
    MM(MergeSorting, merge_sorting) \
    MM(MergingAggregated, merging_aggregated) \
    MM(MergingSorted, merging_sorted) \
    MM(PartialSorting, partial_sorting) \
    MM(PartitionTopN, partition_top_n) \
    MM(Projection, projection) \
    MM(Expand, expand) \
    MM(ReadNothing, read_nothing) \
    MM(ReadStorageRowCount, read_storage_row_count) \
    MM(RemoteExchangeSource, remote_exchange_source) \
    MM(Sorting, sorting) \
    MM(TableFinish, table_finish) \
    MM(TableScan, table_scan) \
    MM(TableWrite, table_write) \
    MM(TopNFiltering, top_n_filtering) \
    MM(Union, union) \
    MM(Window, window) \
    MM(Values, values) \
    MM(IntersectOrExcept, intersect_or_except) \
    MM(Buffer, buffer)\
    MM(Apply, apply) \
    MM(ExplainAnalyze, explain_analyze) \
    MM(FinalSample, final_sample) \
    MM(Offset, offset) \
    MM(FinishSorting, finish_sorting) \
    MM(TotalsHaving, totals_having) \
    MM(OutfileWrite, outfile_write) \
    MM(OutfileFinish, outfile_finish) \
    MM(LocalExchange, local_exchange) \
    MM(IntermediateResultCache, intermediate_result_cache) \
    MM(MultiJoin, multi_join)

// macro helpers to convert MM(x, y) to M(x)
#define IMPL_TUPLE_TO_FIRST(_x, _y) (_x)
#define IMPL_STEPS_PROTOBUF_TYPES APPLY_STEP_PROTOBUF_TYPES_AND_NAMES(IMPL_TUPLE_TO_FIRST)
#define IMPL_MACRO_FUNCTION_APPLY(_r, _data, _elem) _data(_elem)

// apply unary macro
// M(Apply) M(Join) M(Aggregating)...
#define APPLY_STEP_TYPES(M) BOOST_PP_SEQ_FOR_EACH(IMPL_MACRO_FUNCTION_APPLY, M, IMPL_STEPS_PROTOBUF_TYPES)

struct RuntimeAttributeDescription
{
    String description;
    std::vector<std::pair<String, String>> name_and_detail;
    // If the attribute information is complex, can use json
    String additional;
    //TODO:
    // void fillFromProto(const Protos::RuntimeAttributeDescription & proto);
    // void toProto(Protos::RuntimeAttributeDescription & proto) const;
};

class IQueryPlanStepExt : public IQueryPlanStep
{
public:
    std::unordered_map<String, RuntimeAttributeDescription> & getAttributeDescriptions()
    {
        return attribute_descriptions;
    }
protected:
    /// Text description of runtime attributes
    std::unordered_map<String, RuntimeAttributeDescription> attribute_descriptions;
};

}
