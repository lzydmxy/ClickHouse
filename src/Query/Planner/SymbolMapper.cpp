#include <Query/Planner/SymbolMapper.h>

#include <Common/Exception.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Query/Processors/QueryPlan/AssignUniqueIdStepExt.h>
#include <Query/Processors/QueryPlan/DistinctStepExt.h>
#include <Query/Optimizer/SimpleExpressionRewriter.h>
#include <Query/Optimizer/CardinalityEstimate/PlanNodeStatisticsEstimate.h>
#include <Query/Processors/Transforms/AggregatingTransformExt.h>
#include <Query/Processors/QueryPlan/PlanVisitor.h>
#include <Query/Common/Void.h>

#include <memory>

namespace DB
{
static constexpr size_t MAX_LOOKUP_TIMES = 10000;

class SymbolMapper::IdentifierRewriter : public SimpleExpressionRewriter<Void>
{
public:
    explicit IdentifierRewriter(MappingFunction & mapping_function_) : mapping_function(mapping_function_) { }

    ASTPtr visitASTIdentifier(ASTPtr & expr, Void &) override
    {
        return std::make_shared<ASTIdentifier>(mapping_function(expr->as<ASTIdentifier &>().name()));
    }

private:
    MappingFunction & mapping_function;
};

SymbolMapper SymbolMapper::simpleMapper(std::unordered_map<Symbol, Symbol> & mapping)
{
    return SymbolMapper([&mapping](Symbol symbol) {
        auto it = mapping.find(symbol);
        return it != mapping.end() ? it->second : std::move(symbol);
    });
}

SymbolMapper SymbolMapper::symbolMapper(std::unordered_map<Symbol, Symbol> & mapping)
{
    return SymbolMapper([&mapping](Symbol symbol) {
        auto it = mapping.find(symbol);
        size_t lookup = 0;
        while (it != mapping.end() && it->second != symbol)
        {
            if (++lookup > MAX_LOOKUP_TIMES)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "endless loop in SymbolMapper");
            symbol = it->second;
            it = mapping.find(symbol);
        }
        return symbol;
    });
}

SymbolMapper SymbolMapper::symbolReallocator(std::unordered_map<Symbol, Symbol> & mapping, SymbolAllocator & symbolAllocator)
{
    return SymbolMapper([&](Symbol symbol) {
        auto it = mapping.find(symbol);
        if (it != mapping.end())
        {
            size_t lookup = 0;
            while (it != mapping.end() && it->second != symbol)
            {
                if (++lookup > MAX_LOOKUP_TIMES)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "endless loop in SymbolMapper");
                symbol = it->second;
                it = mapping.find(symbol);
            }
            // do not remap the symbol further
            mapping[symbol] = symbol;
            return symbol;
        }

        Symbol new_symbol = symbolAllocator.newSymbol(symbol);
        mapping[symbol] = new_symbol;
        // do not remap the symbol further
        mapping[new_symbol] = new_symbol;
        return new_symbol;
    });
}

NameSet SymbolMapper::mapToDistinct(const Names & symbols)
{
    NameSet ret;
    std::transform(symbols.begin(), symbols.end(), std::inserter(ret, ret.end()), mapping_function);
    return ret;
}

NameSet SymbolMapper::map(const NameSet & names)
{
    NameSet ret;
    std::transform(names.begin(), names.end(), std::inserter(ret, ret.end()), mapping_function);
    return ret;
}


NamesAndTypes SymbolMapper::map(const NamesAndTypes & name_and_types)
{
    NamesAndTypes ret;
    std::transform(name_and_types.begin(), name_and_types.end(), std::back_inserter(ret), [&](const auto & name_and_type) {
        return NameAndTypePair{mapping_function(name_and_type.name), name_and_type.type};
    });
    return ret;
}

Assignments SymbolMapper::map(const Assignments & assignments)
{
    Assignments ret;
    for (const auto & assignment : assignments)
    {
        auto output = map(assignment.first);
        // fixme: handle duplicate assignment
        if (!ret.contains(output))
        {
            ret.emplace(output, map(assignment.second));
        }
    }
    return ret;
}

Assignment SymbolMapper::map(const Assignment & assignment)
{
    return {map(assignment.first), map(assignment.second)};
}

NameToType SymbolMapper::map(const NameToType & name_to_type)
{
    NameToType ret;
    for (const auto & [name, type] : name_to_type)
    {
        ret.emplace(map(name), type);
    }
    return ret;
}

NamesWithAliases SymbolMapper::map(const NamesWithAliases & name_with_aliases)
{
    NamesWithAliases ret;
    std::transform(name_with_aliases.begin(), name_with_aliases.end(), std::back_inserter(ret), [&](const auto & name_with_alias) {
        return NameWithAlias{name_with_alias.first, mapping_function(name_with_alias.second)};
    });
    return ret;
}

Block SymbolMapper::map(const Block & name_and_types)
{
    Block ret;
    for (const auto & item : name_and_types)
    {
        auto mapped = mapping_function(item.name);
        if (!ret.has(mapped))
        {
            ret.insert(ColumnWithTypeAndName{item.column, item.type, mapped});
        }
    }
    return ret;
}

DataStream SymbolMapper::map(const DataStream & data_stream)
{
    DataStream output{map(data_stream.header)};
    output.has_single_port = data_stream.has_single_port;
    output.sort_scope = data_stream.sort_scope;
    output.sort_description = SortDescription{map(data_stream.sort_description)};
    return output;
}

ASTPtr SymbolMapper::map(const ASTPtr & expr)
{
    if (expr == nullptr)
    {
        return nullptr;
    }
    IdentifierRewriter visitor(mapping_function);
    Void void_context{};
    return ASTVisitorUtil::accept(expr->clone(), visitor, void_context);
}

ASTPtr SymbolMapper::map(const ConstASTPtr & expr)
{
    if (expr == nullptr)
    {
        return nullptr;
    }
    IdentifierRewriter visitor(mapping_function);
    Void void_context{};
    return ASTVisitorUtil::accept(expr->clone(), visitor, void_context);
}

Partitioning SymbolMapper::map(const Partitioning & partition)
{
    return {
        partition.getHandle(),
        map(partition.getColumns()),
        partition.isRequireHandle(),
        partition.getBuckets(),
        partition.getBucketExpr(),
        partition.isEnforceRoundRobin(),
        partition.getComponent()};
}

std::shared_ptr<JoinStepExt> SymbolMapper::map(const JoinStepExt & join)
{
    return std::make_shared<JoinStepExt>(
        map(join.getInputStreams()),
        map(join.getOutputStream()),
        join.getKind(),
        join.getStrictness(),
        join.getMaxStreams(),
        join.getKeepLeftReadInOrder(),
        map(join.getLeftKeys()),
        map(join.getRightKeys()),
        join.getKeyIdsNullSafe(),
        map(join.getFilter()),
        join.isHasUsing(),
        join.getRequireRightKeys(),
        join.getAsofInequality(),
        join.getDistributionType(),
        join.getJoinAlgorithm(),
        join.isMagic(),
        join.isOrdered(),
        join.isSimpleReordered(),
        map(join.getRuntimeFilterBuilders()));
}

LinkedHashMap<String, RuntimeFilter> SymbolMapper::map(const LinkedHashMap<String, RuntimeFilter> & infos)
{
    LinkedHashMap<String, RuntimeFilter> res;
    for (const auto & info : infos)
    {
        auto symbol = map(info.first);
        // fixme: handle duplicate symbol
        if (!res.contains(symbol))
        {
            res.emplace(symbol, info.second);
        }
    }
    return res;
}

PlanNodeStatisticsEstimate SymbolMapper::map(const PlanNodeStatisticsEstimate & estimate)
{
    if (!estimate.has_value())
    {
        return estimate;
    }

    std::unordered_map<String, SymbolStatisticsPtr> symbol_statistics;
    for (const auto & entry : estimate.value()->getSymbolStatistics())
    {
        symbol_statistics.emplace(map(entry.first), entry.second);
    }
    return PlanNodeStatisticsEstimate{
        std::make_optional(std::make_shared<PlanNodeStatistics>(estimate.value()->getRowCount(), std::move(symbol_statistics)))};
}


AggregateDescription SymbolMapper::map(const AggregateDescription & desc)
{
    return AggregateDescription{
        desc.function,
        desc.parameters,
        map(desc.argument_names),
        map(desc.column_name),
        desc.arguments,
        desc.mask_column.empty() ? desc.mask_column : map(desc.mask_column)};
}

WindowFunctionDescription SymbolMapper::map(const WindowFunctionDescription & desc)
{
    return WindowFunctionDescription{
        map(desc.column_name),
        desc.function_node,
        desc.aggregate_function,
        desc.function_parameters,
        desc.argument_types,
        map(desc.argument_names)};
}

WindowDescription SymbolMapper::map(const WindowDescription & desc)
{
    return WindowDescription{
        desc.window_name,
        SortDescription{map(desc.partition_by)},
        SortDescription{map(desc.order_by)},
        SortDescription{map(desc.full_sort_description)},
        desc.partition_by_actions,
        desc.order_by_actions,
        desc.frame,
        map(desc.window_functions)};
}

SortColumnDescription SymbolMapper::map(const SortColumnDescription & desc)
{
    auto res = desc;
    res.column_name = map(desc.column_name);
    return res;
}

GroupingDescription SymbolMapper::map(const GroupingDescription & desc)
{
    return GroupingDescription{map(desc.argument_names), map(desc.output_name)};
}

GroupingSetsParamsExt SymbolMapper::map(const GroupingSetsParamsExt & param)
{
    GroupingSetsParamsExt res{map(param.used_key_names)};
    res.used_keys = param.used_keys;
    res.missing_keys = param.missing_keys;
    return res;
}

AggregatorExt::Params SymbolMapper::map(const AggregatorExt::Params & params)
{
    auto header = map(params.src_header);
    auto intermediate_header = map(params.intermediate_header);
    ColumnNumbers keys;
    std::unordered_set<String> distinct_keys;
    if (params.src_header.columns() != 0)
    {
        for (const auto & key : params.keys)
        {
            auto name = map(params.src_header.getByPosition(key).name);
            if (distinct_keys.emplace(name).second)
                keys.emplace_back(header.getPositionByName(name));
        }
    }
    else
    {
        for (const auto & key : params.keys)
        {
            auto name = map(params.intermediate_header.getByPosition(key).name);
            if (distinct_keys.emplace(name).second)
                keys.emplace_back(intermediate_header.getPositionByName(name));
        }
    }

    return AggregatorExt::Params{
        header,
        keys,
        map(params.aggregates),
        params.overflow_row,
        params.max_rows_to_group_by,
        params.group_by_overflow_mode,
        params.group_by_two_level_threshold,
        params.group_by_two_level_threshold_bytes,
        params.max_bytes_before_external_group_by,
        params.enable_adaptive_spill,
        params.spill_buffer_bytes_before_external_group_by,
        params.empty_result_for_aggregation_by_empty_set,
        params.tmp_data_scope,
        params.max_threads,
        params.min_free_disk_space,
        params.compile_aggregate_expressions,
        params.min_count_to_compile_aggregate_expression,
        params.max_block_size,
        params.enable_prefetch,
        params.only_merge,
        params.optimize_group_by_constant_keys,
        params.min_hit_rate_to_use_consecutive_keys_optimization,
        params.stats_collecting_params,
        intermediate_header,
        params.enable_lc_group_by_opt};
}

Aggregator::Params SymbolMapper::map(const Aggregator::Params & params)
{
    auto keys = map(params.keys);
    std::unordered_set<String> distinct_keys;

    return Aggregator::Params{
        keys,
        map(params.aggregates),
        params.overflow_row,
        params.max_rows_to_group_by,
        params.group_by_overflow_mode,
        params.group_by_two_level_threshold,
        params.group_by_two_level_threshold_bytes,
        params.max_bytes_before_external_group_by,
        params.empty_result_for_aggregation_by_empty_set,
        params.tmp_data_scope,
        params.max_threads,
        params.min_free_disk_space,
        params.compile_aggregate_expressions,
        params.min_count_to_compile_aggregate_expression,
        params.max_block_size,
        params.enable_prefetch,
        params.only_merge,
        params.optimize_group_by_constant_keys,
        params.min_hit_rate_to_use_consecutive_keys_optimization,
        params.stats_collecting_params};
}

AggregatingTransformParamsExtPtr SymbolMapper::map(const AggregatingTransformParamsExtPtr & param)
{
    if (param->aggregator_ext_list_ptr && param->aggregator_ext_list_ptr->size() > 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Symbol mapper is unable to handle parallel aggregate param.");

    return std::make_shared<AggregatingTransformParamsExt>(map(param->params), param->final);
}

ArrayJoinActionPtr SymbolMapper::map(const ArrayJoinActionPtr & array_join_action)
{
    if (array_join_action == nullptr)
        return nullptr;
    return std::make_shared<ArrayJoinAction>(*array_join_action);
}

SortDescription SymbolMapper::map(const SortDescription & sort_desc)
{
    SortDescription res;
    std::transform(sort_desc.begin(), sort_desc.end(), std::back_inserter(res), [this](const auto & param) { return this->map(param); });
    return res;
}

SortColumnDescriptionWithColumnIndex SymbolMapper::map(const SortColumnDescriptionWithColumnIndex & sort_column_description)
{
    return SortColumnDescriptionWithColumnIndex{map(sort_column_description.base), sort_column_description.column_number};
}

std::map<Int32, Names> SymbolMapper::map(const std::map<Int32, Names> & group_id_non_null_symbol)
{
    std::map<Int32, Names> res;
    for (const auto & entry : group_id_non_null_symbol)
    {
        res[entry.first] = map(entry.second);
    }
    return res;
}

std::shared_ptr<AggregatingStepExt> SymbolMapper::map(const AggregatingStepExt & agg)
{
    return std::make_shared<AggregatingStepExt>(
        map(agg.getInputStreams()[0]),
        distinct(map(agg.getKeys())),
        map(agg.getKeysNotHashed()),
        map(agg.getAggregates()),
        map(agg.getGroupingSetsParams()),
        agg.isFinal(),
        agg.getStagePolicy(),
        SortDescriptionWithPositions{map(agg.getGroupBySortDescription())},
        map(agg.getGroupings()),
        agg.needOverflowRow(),
        agg.shouldProduceResultsInOrderOfBucketNumber(),
        agg.isNoShuffle(),
        agg.isStreamingForCache());
}

std::shared_ptr<ApplyStepExt> SymbolMapper::map(const ApplyStepExt & apply)
{
    return std::make_shared<ApplyStepExt>(
        map(apply.getInputStreams()),
        map(apply.getCorrelation()),
        apply.getApplyType(),
        apply.getSubqueryType(),
        map(apply.getAssignment()),
        map(apply.getOuterColumns()),
        apply.supportSemiAnti());
}

std::shared_ptr<ArrayJoinStep> SymbolMapper::map(const ArrayJoinStep & array_join)
{
    return std::make_shared<ArrayJoinStep>(map(array_join.getInputStreams()[0]), map(array_join.arrayJoin()));
}

std::shared_ptr<AssignUniqueIdStepExt> SymbolMapper::map(const AssignUniqueIdStepExt & assign)
{
    return std::make_shared<AssignUniqueIdStepExt>(map(assign.getInputStreams()[0]), map(assign.getUniqueId()));
}

std::shared_ptr<DistinctStepExt> SymbolMapper::map(const DistinctStepExt & distinct)
{
    return std::make_shared<DistinctStepExt>(
        map(distinct.getInputStreams()[0]),
        distinct.getSetSizeLimits(),
        distinct.getLimitHint(),
        map(distinct.getColumns()),
        distinct.preDistinct(),
        false,
        distinct.canToAgg());
}

std::shared_ptr<EnforceSingleRowStepExt> SymbolMapper::map(const EnforceSingleRowStepExt & row)
{
    return std::make_shared<EnforceSingleRowStepExt>(map(row.getInputStreams()[0]));
}

std::shared_ptr<ExtremesStep> SymbolMapper::map(const ExtremesStep & extremes)
{
    return std::make_shared<ExtremesStep>(extremes.getInputStreams()[0]);
}

std::shared_ptr<ExceptStepExt> SymbolMapper::map(const ExceptStepExt & except)
{
    std::unordered_map<String, std::vector<String>> outputs_to_inputs;
    for (const auto & [output, inputs] : except.getOutToInputs())
    {
        auto mapped_inputs = map(inputs);
        outputs_to_inputs.emplace(map(output), mapped_inputs);
    }
    return std::make_shared<ExceptStepExt>(
        map(except.getInputStreams()), map(except.getOutputStream()), outputs_to_inputs, except.isDistinct());
}

std::shared_ptr<ExchangeStepExt> SymbolMapper::map(const ExchangeStepExt & exchange)
{
    return std::make_shared<ExchangeStepExt>(
        map(exchange.getInputStreams()), exchange.getExchangeMode(), map(exchange.getSchema()), exchange.needKeepOrder());
}

std::shared_ptr<FillingStep> SymbolMapper::map(const FillingStep & filling)
{
    return std::make_shared<FillingStep>(map(filling.getInputStreams()[0]), SortDescription{map(filling.getSortDescription())}, SortDescription{map(filling.getSortDescription())}, nullptr, false);
}

std::shared_ptr<FinalSampleStepExt> SymbolMapper::map(const FinalSampleStepExt & final_sample)
{
    return std::make_shared<FinalSampleStepExt>(
        map(final_sample.getInputStreams()[0]), final_sample.getSampleSize(), final_sample.getMaxChunkSize());
}



std::shared_ptr<IntersectOrExceptStep> SymbolMapper::map(const IntersectOrExceptStep & intersect_or_except)
{
    return std::make_shared<IntersectOrExceptStep>(
        map(intersect_or_except.getInputStreams()), QueryPlanStepHelper::getIntersectOrExceptStepOperator(intersect_or_except), QueryPlanStepHelper::getIntersectOrExceptStepMaxThreads(intersect_or_except));
}

std::shared_ptr<TableScanStepExt> SymbolMapper::map(const TableScanStepExt & scan)
{
    DataStream mapped_output_stream = map(scan.getOutputStream());
    NamesWithAliases mapped_column_alias = map(scan.getColumnAlias());
    DataStream mapped_table_output_stream = map(scan.getTableOutputStream());

    Assignments mapped_inline_expressions;

    for (const auto & inline_expr : scan.getInlineExpressions())
        mapped_inline_expressions.emplace_back(mapping_function(inline_expr.first), inline_expr.second);

    // order matters as symbol mapper should traverse plan nodes bottom-up
    std::shared_ptr<FilterStepExt> mapped_filter = scan.getPushdownFilterCast() ? map(*scan.getPushdownFilterCast()) : nullptr;
    std::shared_ptr<ProjectionStepExt> mapped_projection = scan.getPushdownProjectionCast() ? map(*scan.getPushdownProjectionCast()) : nullptr;
    std::shared_ptr<AggregatingStepExt> mapped_aggregation
        = scan.getPushdownAggregationCast() ? map(*scan.getPushdownAggregationCast()) : nullptr;

    auto mapped_scan = std::make_shared<TableScanStepExt>(
        std::move(mapped_output_stream),
        scan.getStorage(),
        scan.getStorageID(),
        scan.getMetadataSnapshot(),
        scan.getStorageSnapshot(),
        scan.getOriginalTable(),
        scan.getColumnNames(),
        std::move(mapped_column_alias),
        scan.getQueryInfo(),
        scan.getMaxBlockSize(),
        scan.getTableAlias(),
        scan.isBucketScan(),
        std::move(mapped_inline_expressions),
        std::move(mapped_aggregation),
        std::move(mapped_projection),
        std::move(mapped_filter),
        std::move(mapped_table_output_stream));

    return mapped_scan;
}


std::shared_ptr<FilterStepExt> SymbolMapper::map(const FilterStepExt & filter)
{
    return std::make_shared<FilterStepExt>(map(filter.getInputStreams()[0]), map(filter.getFilter()), filter.removesFilterColumn());
}

std::shared_ptr<LimitStepExt> SymbolMapper::map(const LimitStepExt & limit)
{
    return std::make_shared<LimitStepExt>(
        map(limit.getInputStreams()[0]),
        limit.getLimit(),
        limit.getOffset(),
        limit.isAlwaysReadTillEnd(),
        limit.isWithTies(),
        SortDescription{map(limit.getSortDescription())},
        limit.isPartial());
}

std::shared_ptr<LimitByStep> SymbolMapper::map(const LimitByStep & limit)
{
    Names names = {map(QueryPlanStepHelper::getLimitByStepColumns(limit))};
    return std::make_shared<LimitByStep>(map(limit.getInputStreams()[0]), QueryPlanStepHelper::getLimitByStepGroupLength(limit), QueryPlanStepHelper::getLimitByStepGroupOffset(limit), map(names));
}

std::shared_ptr<MergingSortedStepExt> SymbolMapper::map(const MergingSortedStepExt & sorted)
{
    return std::make_shared<MergingSortedStepExt>(
        map(sorted.getInputStreams()[0]), SortDescription{map(sorted.getSortDescription())}, sorted.getMaxBlockSize(), sorted.getLimit());
}

std::shared_ptr<MergingAggregatedStepExt> SymbolMapper::map(const MergingAggregatedStepExt & merging_agg)
{
    const auto & agg_params = merging_agg.getParams();
    Aggregator::Params new_params(
        merging_agg.getKeys(),
        agg_params.aggregates,
        agg_params.overflow_row,
        agg_params.max_threads,
        agg_params.max_block_size,
        agg_params.min_hit_rate_to_use_consecutive_keys_optimization);

    return std::make_shared<MergingAggregatedStepExt>(
        map(merging_agg.getInputStreams()[0]),
        map(merging_agg.getGroupingSetsParamsList()),
        map(merging_agg.getGroupings()),
        merging_agg.isFinal(),
        map(merging_agg.getParams()),
        merging_agg.isMemoryEfficientAggregation(),
        merging_agg.getMaxThreads(),
        merging_agg.getMemoryEfficientMergeThreads(),
        merging_agg.getMaxBlockSize(),
        merging_agg.getMemoryBoundMergingMaxBlockBytes(),
        map(merging_agg.getGroupBySortDescription()),
        merging_agg.getMemoryBoundMergingOfAggregationResultsEnabled());
}


std::shared_ptr<MergeSortingStepExt> SymbolMapper::map(const MergeSortingStepExt & sorting)
{
    return std::make_shared<MergeSortingStepExt>(
        map(sorting.getInputStreams()[0]),
        SortDescription{map(sorting.getSortDescription())},
        sorting.getMaxMergedBlockSize(),
        sorting.getLimit(),
        sorting.getMaxBytesBeforeRemerge(),
        sorting.getRemergeLoweredMemoryBytesRatio(),
        sorting.getMaxBytesBeforeExternalSort(),
        sorting.getTmpData(),
        sorting.getMinFreeDiskSpace());
}

std::shared_ptr<MarkDistinctStepExt> SymbolMapper::map(const MarkDistinctStepExt & mark_distinct)
{
    return std::make_shared<MarkDistinctStepExt>(
        map(mark_distinct.getInputStreams()[0]), map(mark_distinct.getMarkerSymbol()), map(mark_distinct.getDistinctSymbols()));
}


std::shared_ptr<OffsetStep> SymbolMapper::map(const OffsetStep & offset)
{
    return std::make_shared<OffsetStep>(map(offset.getInputStreams()[0]), QueryPlanStepHelper::getOffsetStepOffset(offset));
}

std::shared_ptr<PartitionTopNStepExt> SymbolMapper::map(const PartitionTopNStepExt & partition_topn)
{
    return std::make_shared<PartitionTopNStepExt>(
        map(partition_topn.getInputStreams()[0]),
        map(partition_topn.getPartition()),
        map(partition_topn.getOrderBy()),
        partition_topn.getLimit(),
        partition_topn.getModel());
}

std::shared_ptr<PartialSortingStepExt> SymbolMapper::map(const PartialSortingStepExt & partial_sorting)
{
    return std::make_shared<PartialSortingStepExt>(
        map(partial_sorting.getInputStreams()[0]),
        SortDescription{partial_sorting.getSortDescription()},
        partial_sorting.getLimit(),
        partial_sorting.getSizeLimits());
}

std::shared_ptr<FinishSortingStepExt> SymbolMapper::map(const FinishSortingStepExt & finish_sorting)
{
    return std::make_shared<FinishSortingStepExt>(
        map(finish_sorting.getInputStreams()[0]),
        SortDescription{map(finish_sorting.getPrefixDescription())},
        SortDescription{map(finish_sorting.getResultDescription())},
        finish_sorting.getMaxBlockSize(),
        finish_sorting.getLimit());
}

std::shared_ptr<IntersectStepExt> SymbolMapper::map(const IntersectStepExt & intersect)
{
    std::unordered_map<String, std::vector<String>> outputs_to_inputs;
    for (const auto & [output, inputs] : intersect.getOutToInputs())
    {
        auto mapped_inputs = map(inputs);
        outputs_to_inputs.emplace(map(output), mapped_inputs);
    }
    return std::make_shared<IntersectStepExt>(
        map(intersect.getInputStreams()), map(intersect.getOutputStream()), outputs_to_inputs, intersect.isDistinct());
}

std::shared_ptr<ProjectionStepExt> SymbolMapper::map(const ProjectionStepExt & projection)
{
    if (projection.isFinalProject())
    {
        Assignments assignments;
        for (const auto & item : projection.getAssignments())
            assignments.emplace(item.first, map(item.second));

        return std::make_shared<ProjectionStepExt>(
            map(projection.getInputStreams()[0]),
            std::move(assignments),
            projection.getNameToType(),
            projection.isFinalProject(),
            projection.isIndexProject());
    }
    return std::make_shared<ProjectionStepExt>(
        map(projection.getInputStreams()[0]),
        map(projection.getAssignments()),
        map(projection.getNameToType()),
        projection.isFinalProject(),
        projection.isIndexProject());
}

std::shared_ptr<ReadNothingStep> SymbolMapper::map(const ReadNothingStep & read_nothing)
{
    return std::make_shared<ReadNothingStep>(map(read_nothing.getOutputStream()).header);
}

std::shared_ptr<RemoteExchangeSourceStepExt> SymbolMapper::map(const RemoteExchangeSourceStepExt & remote_exchange)
{
    return std::make_shared<RemoteExchangeSourceStepExt>(
        remote_exchange.getInput(),
        map(remote_exchange.getInputStreams()[0]),
        remote_exchange.isAddTotals(),
        remote_exchange.isAddExtremes());
}

std::shared_ptr<SortingStepExt> SymbolMapper::map(const SortingStepExt & sorting)
{
    return std::make_shared<SortingStepExt>(
        map(sorting.getInputStreams()[0]),
        SortDescription{map(sorting.getSortDescription())},
        sorting.getLimit(),
        sorting.getStage(),
        SortDescription{map(sorting.getPrefixDescription())});
}

std::shared_ptr<TopNFilteringStepExt> SymbolMapper::map(const TopNFilteringStepExt & topn_filter)
{
    return std::make_shared<TopNFilteringStepExt>(
        map(topn_filter.getInputStreams()[0]),
        map(topn_filter.getSortDescription()),
        topn_filter.getSize(),
        topn_filter.getModel(),
        topn_filter.getAlgorithm());
}


std::shared_ptr<UnionStepExt> SymbolMapper::map(const UnionStepExt & union_)
{
    std::unordered_map<String, std::vector<String>> outputs_to_inputs;
    for (const auto & [output, inputs] : union_.getOutToInputs())
    {
        auto mapped_inputs = map(inputs);
        outputs_to_inputs.emplace(map(output), mapped_inputs);
    }
    return std::make_shared<UnionStepExt>(
        map(union_.getInputStreams()), map(union_.getOutputStream()), outputs_to_inputs, union_.getMaxThreads(), union_.isLocal());
}

std::shared_ptr<ValuesStepExt> SymbolMapper::map(const ValuesStepExt & values)
{
    return std::make_shared<ValuesStepExt>(map(values.getOutputStream().header), values.getFields(), values.getRows());
}

std::shared_ptr<WindowStep> SymbolMapper::map(const WindowStep & window)
{
    return std::make_shared<WindowStep>(
        map(window.getInputStreams()[0]),
        map(window.getWindowDescription()),
        map(QueryPlanStepHelper::getWindowStepFunctions(window)),
        QueryPlanStepHelper::getWindowStepStreamsFanOut(window));
}

std::shared_ptr<CTERefStepExt> SymbolMapper::map(const CTERefStepExt & cte_ref)
{
    std::unordered_map<String, String> output_columns;
    for (const auto & item : cte_ref.getOutputColumns())
    {
        output_columns.emplace(map(item.first), item.second);
    }
    return std::make_shared<CTERefStepExt>(map(cte_ref.getOutputStream()), cte_ref.getId(), output_columns, cte_ref.hasFilter());
}

std::shared_ptr<ExplainAnalyzeStepExt> SymbolMapper::map(const ExplainAnalyzeStepExt & step)
{
    return std::make_shared<ExplainAnalyzeStepExt>(
        map(step.getInputStreams()[0]),
        map(step.getOutputName()),
        step.getKind(),
        step.getContext(),
        step.getQueryPlan(),
        step.getSetting());
}

std::shared_ptr<LocalExchangeStepExt> SymbolMapper::map(const LocalExchangeStepExt & step)
{
    return std::make_shared<LocalExchangeStepExt>(map(step.getInputStreams()[0]), step.getExchangeMode(), map(step.getSchema()));
}

std::shared_ptr<ReadStorageRowCountStepExt> SymbolMapper::map(const ReadStorageRowCountStepExt & step)
{
    auto new_step = std::make_shared<ReadStorageRowCountStepExt>(
        map(step.getOutputStream().header), step.getQuery(), step.getAggregateDescription(), step.isFinal(), step.getStorageID(), step.getContext());
    new_step->setNumRows(step.getNumRows());
    return new_step;
}

std::shared_ptr<BufferStepExt> SymbolMapper::map(const BufferStepExt & step)
{
    return std::make_shared<BufferStepExt>(map(step.getInputStreams()[0]));
}

std::shared_ptr<IntermediateResultCacheStepExt> SymbolMapper::map(const IntermediateResultCacheStepExt & step)
{
    return std::make_shared<IntermediateResultCacheStepExt>(map(step.getInputStreams()[0]), step.getAggregatorParams());
}

std::shared_ptr<MultiJoinStepExt> SymbolMapper::map(const MultiJoinStepExt & step)
{
    return std::make_shared<MultiJoinStepExt>(map(step.getOutputStream()), step.getGraph());
}

std::shared_ptr<TotalsHavingStepExt> SymbolMapper::map(const TotalsHavingStepExt & step)
{

    AggregateDescriptions aggregate_descriptions;

    return std::make_shared<TotalsHavingStepExt>(
        map(step.getInputStreams()[0]),
        aggregate_descriptions,
        step.isOverflowRow(),
        map(step.getHavingFilter()),
        nullptr,
        "",
        false,
        step.getTotalsMode(),
        step.getAutoIncludeThreshols(),
        true);
}

std::shared_ptr<ExpandStepExt> SymbolMapper::map(const ExpandStepExt & step)
{
    return std::make_shared<ExpandStepExt>(
        map(step.getOutputStream()),
        map(step.getAssignments()),
        map(step.getNameToType()),
        map(step.getGroupIdSymbol()),
        step.getGroupIdValue(),
        map(step.getGroupIdNonNullSymbol()));
}

class SymbolMapper::SymbolMapperVisitor : public StepVisitor<QueryPlanStepPtr, SymbolMapper>
{
protected:
#define VISITOR_DEF(TYPE) \
    QueryPlanStepPtr visit##TYPE(const TYPE & step, SymbolMapper & mapper) override { return mapper.map(step); }
    APPLY_PROTOBUF_STEP_TYPES(VISITOR_DEF)
#undef VISITOR_DEF
};

QueryPlanStepPtr SymbolMapper::map(const IQueryPlanStep & step)
{
    SymbolMapperVisitor visitor;
    return VisitorUtil::accept(step, visitor, *this);
}
}
