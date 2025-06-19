#include <Query/Optimizer/CardinalityEstimate/FilterEstimator.h>
#include <Query/Optimizer/CardinalityEstimate/TableScanEstimator.h>
#include <Query/Optimizer/ExpressionDeterminism.h>
#include <Query/Optimizer/PredicateUtils.h>
#include <Query/Optimizer/Rule/Patterns.h>
#include <Query/Optimizer/Rule/Rewrite/PushIntoTableScanRules.h>
#include <Query/Optimizer/SymbolsExtractor.h>
#include <Query/Optimizer/Utils.h>
#include <Query/Processors/QueryPlan/LimitStepExt.h>
#include <Query/Planner/SymbolMapper.h>
#include <Query/Processors/QueryPlan/TableScanStepExt.h>
#include <Query/Optimizer/SelectQueryInfoHelper.h>
#include <Query/Interpreters/PartitionPredicateVisitor.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeWhereOptimizer.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/StorageDistributed.h>

namespace DB
{

namespace
{
    bool isOptimizerProjectionSupportEnabled(RuleContext & rule_context)
    {
        return rule_context.context->getOptimizerContext()->getSettingsRef().optimizer_projection_support;
    }

    bool isOptimizerIndexProjectionSupportEnabled(RuleContext & rule_context)
    {
        return rule_context.context->getOptimizerContext()->getSettingsRef().optimizer_index_projection_support;
    }
}

ConstRefPatternPtr PushStorageFilter::getPattern() const
{
    static auto pattern = Patterns::filter().withSingle(Patterns::tableScan().matchingStep<TableScanStepExt>([](const auto & step) {
            // check repeat calls
            //todo: liyang453, other feat: need partition_filter in query_info
            //const auto & query_info = step.getQueryInfo();
            //const auto * select_query = query_info.getSelectQuery();
            //return !(select_query->where());

            ASTSelectQuery select_query;
            return !(select_query.where());
        }))
        .result();
    return pattern;
}

TransformResult PushStorageFilter::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto table_scan = node->getChildren()[0];

    const auto * filter_step = dynamic_cast<const FilterStepExt *>(node->getStep().get());
    auto copy_table_step = QueryPlanStepHelper::copyQueryPlanStep(table_scan->getStep(), rule_context.context);

    // try get statistics of base table
    PlanNodeStatisticsPtr stat;
    if (rule_context.context->getOptimizerContext()->getSettingsRef().enable_active_prewhere)
    {
        if (table_scan->getStatistics().has_value())
            stat = table_scan->getStatistics().value();
        else
            stat = TableScanEstimator::estimate(rule_context.context, static_cast<const TableScanStepExt &>(*table_scan->getStep()));
    }

    // TODO: check repeat calls by checking query_info has been set
    auto remaining_filter
        = pushStorageFilter(dynamic_cast<TableScanStepExt &>(*copy_table_step), filter_step->getFilter()->clone(), stat, rule_context.context);
    table_scan->setStep(copy_table_step);

    if (PredicateUtils::isTruePredicate(remaining_filter))
        return table_scan;

    if (ASTEquality::ASTEquals()(filter_step->getFilter(), remaining_filter))
        return {};

    auto new_filter_step = std::make_shared<FilterStepExt>(table_scan->getStep()->getOutputStream(), remaining_filter, filter_step->removesFilterColumn());
    return PlanNodeBase::createPlanNode(rule_context.context->getOptimizerContext()->nextNodeId(), std::move(new_filter_step), PlanNodes{table_scan}, node->getStatistics());
}


ASTPtr applyFilter(ASTPtr query_filter, SelectQueryInfo & query_info, ContextPtr, PlanNodeStatisticsPtr)
{
    // only set query.where()
    if (!query_info.query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query info query is not set");

    auto * select_query = query_info.query->as<ASTSelectQuery>();
    if (!select_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query info query is not a ASTSelectQuery");

    if (!PredicateUtils::isTruePredicate(query_filter))
    {
        if (auto where = select_query->where())
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, PredicateUtils::combineConjuncts(ASTs{query_filter, where}));
        else
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, ASTPtr{query_filter});
    }

    return query_filter;
}

ASTPtr applyFilter(
    TableScanStepExt & table_step, ASTPtr query_filter, ContextPtr query_context, PlanNodeStatisticsPtr storage_statistics)
{
    auto & query_info = table_step.getQueryInfo();
    auto storage = std::dynamic_pointer_cast<MergeTreeData>(table_step.getStorage());

    if (!storage)
    {
        // only set query.where()
        if (!query_info.query)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Query info query is not set");

        auto * select_query = query_info.query->as<ASTSelectQuery>();
        if (!select_query)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Query info query is not a ASTSelectQuery");

        if (!PredicateUtils::isTruePredicate(query_filter))
        {
            if (auto where = select_query->where())
                select_query->setExpression(ASTSelectQuery::Expression::WHERE, PredicateUtils::combineConjuncts(ASTs{query_filter, where}));
            else
                select_query->setExpression(ASTSelectQuery::Expression::WHERE, ASTPtr{query_filter});
        }

        return query_filter;
    }

    const auto & settings = query_context->getOptimizerContext()->getSettingsRef();

    if (!query_info.query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query info query is not set");

    auto * select_query = query_info.query->as<ASTSelectQuery>();
    if (!select_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query info query is not a ASTSelectQuery");

    ASTs conjuncts = PredicateUtils::extractConjuncts(query_filter);

    // todo wujianchao5 support partition pruning

    // Set partition_filter
    // this should be done before setting query.where() to avoid partition filters being chosen as prewhere
    // if (settings.enable_partition_filter_push_down)
    // {
    //     ASTs push_predicates;
    //     ASTs remain_predicates;
    //
    //     Names partition_key_names = storage->getInMemoryMetadataPtr()->getPartitionKey().column_names;
    //     Names virtual_key_names = storage->getInMemoryMetadataPtr()->getSampleBlockWithVirtuals(storage->getVirtualsList()).getNames();
    //     partition_key_names.insert(partition_key_names.end(), virtual_key_names.begin(), virtual_key_names.end());
    //     auto iter = std::stable_partition(conjuncts.begin(), conjuncts.end(), [&](const auto & predicate) {
    //         PartitionPredicateVisitor::Data visitor_data{
    //             query_context, storage, predicate};
    //         PartitionPredicateVisitor(visitor_data).visit(predicate);
    //         return visitor_data.getMatch();
    //     });
    //
    //     push_predicates.insert(push_predicates.end(), conjuncts.begin(), iter);
    //     remain_predicates.insert(remain_predicates.end(), iter, conjuncts.end());
    //
    //     ASTPtr new_partition_filter;
    //
    //     if (query_info.partition_filter)
    //     {
    //         push_predicates.push_back(query_info.partition_filter);
    //         new_partition_filter = PredicateUtils::combineConjuncts(push_predicates);
    //     }
    //     else
    //     {
    //         new_partition_filter = PredicateUtils::combineConjuncts<false>(push_predicates);
    //     }
    //
    //     if (!PredicateUtils::isTruePredicate(new_partition_filter))
    //         query_info.partition_filter = std::move(new_partition_filter);
    //
    //     conjuncts.swap(remain_predicates);
    // }

    /// Set query.where()
    applyFilter(PredicateUtils::combineConjuncts(conjuncts), query_info, query_context, storage_statistics);

    /// Set query.prewhere(), strategy 1: by selectivity
    if (select_query->where() && !select_query->prewhere() && storage->supportsPrewhere() && settings.enable_active_prewhere && storage_statistics)
    {
        auto full_conjuncts = PredicateUtils::extractConjuncts(select_query->getExpression(ASTSelectQuery::Expression::WHERE, true));
        std::vector<ASTPtr> pre_conjuncts;
        std::vector<ASTPtr> where_conjuncts;

        IdentifierNameSet used_columns;
        select_query->getExpression(ASTSelectQuery::Expression::WHERE, true)->collectIdentifierNames(used_columns);
        const auto & columns_desc = storage->getInMemoryMetadataPtr()->getColumns();
        NamesAndTypes names_and_types;
        for (const auto & col_name : used_columns)
            names_and_types.emplace_back(columns_desc.getPhysical(col_name));

        for (const auto & conjunct : full_conjuncts)
        {
            double selectivity = FilterEstimator::estimateFilterSelectivity(storage_statistics, conjunct, names_and_types, query_context);
            LOG_DEBUG(
                ::getLogger("OptimizerActivePrewhere"),
                "conjunct = {}, selectivity = {}", serializeAST(*conjunct), std::to_string(selectivity));

            if (selectivity <= settings.max_active_prewhere_selectivity
                && pre_conjuncts.size() < settings.max_active_prewhere_size)
                pre_conjuncts.push_back(conjunct);
            else
                where_conjuncts.push_back(conjunct);
        }

        if (!pre_conjuncts.empty())
            select_query->setExpression(ASTSelectQuery::Expression::PREWHERE, PredicateUtils::combineConjuncts(pre_conjuncts));

        if (!where_conjuncts.empty())
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, PredicateUtils::combineConjuncts(where_conjuncts));
        else
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, nullptr);
    }

    /// Set query.prewhere(), strategy 2: by IO cost
    if (select_query->where() && !select_query->prewhere() && storage->supportsPrewhere() && settings.enable_optimizer_early_prewhere_push_down)
    {
        /// PREWHERE optimization: transfer some condition from WHERE to PREWHERE if enabled and viable
        if (const auto & column_size = storage->getColumnSizes(); !column_size.empty())
        {
            /// Extract column compressed sizes.
            std::unordered_map<std::string, UInt64> column_compressed_sizes;
            for (const auto & [name, sizes] : column_size)
                column_compressed_sizes[name] = sizes.data_compressed;

            auto current_info = buildSelectQueryInfoForQuery(query_info.query, query_context);

            auto column_sizes = storage->getColumnSizes();
            if (!column_sizes.empty())
            {
                /// Extract column compressed sizes
                std::unordered_map<std::string, UInt64> column_compressed_sizes;
                for (const auto & [name, sizes] : column_sizes)
                    column_compressed_sizes[name] = sizes.data_compressed;

                MergeTreeWhereOptimizer{
                    column_compressed_sizes,
                    table_step.getMetadataSnapshot(),
                    storage->getConditionEstimatorByPredicate(table_step.getQueryInfo(), table_step.getStorageSnapshot(), query_context),
                    current_info.syntax_analyzer_result->requiredSourceColumns(),
                    storage->supportedPrewhereColumns(),
                    ::getLogger("OptimizerEarlyPrewherePushdown")};
            }
        }
    }

    /// remove prewhere from query plan
    if (auto prewhere = select_query->prewhere())
        PredicateUtils::subtract(conjuncts, PredicateUtils::extractConjuncts(prewhere));

    return PredicateUtils::combineConjuncts(conjuncts);
}

ASTPtr PushStorageFilter::pushStorageFilter(TableScanStepExt & table_step, ASTPtr query_filter, PlanNodeStatisticsPtr storage_statistics, ContextMutablePtr context)
{
    std::unordered_map<String, String> column_to_alias;
    for (const auto & item : table_step.getColumnAlias())
        column_to_alias.emplace(item.first, item.second);
    auto alias_to_column = Utils::reverseMap(column_to_alias);
    ASTs conjuncts = PredicateUtils::extractConjuncts(query_filter);

    // split functions into pushable conjuncts & non-pushable conjuncts
    ASTs pushable_conjuncts;
    ASTs non_pushable_conjuncts;
    {
        auto iter = std::stable_partition(conjuncts.begin(), conjuncts.end(), [&](const auto & conjunct) {
            bool all_in = true;
            auto symbols = SymbolsExtractor::extract(conjunct);
            for (const auto & item : symbols)
                all_in &= alias_to_column.contains(item);

            return all_in && ExpressionDeterminism::isDeterministic(conjunct, context);
        });

        pushable_conjuncts.insert(pushable_conjuncts.end(), conjuncts.begin(), iter);
        non_pushable_conjuncts.insert(non_pushable_conjuncts.end(), iter, conjuncts.end());
    }

    // construct push filter by mapping symbol to origin column
    ASTPtr push_filter;
    {
        auto mapper = SymbolMapper::simpleMapper(alias_to_column);
        std::vector<ConstASTPtr> mapped_pushable_conjuncts;
        for (auto & conjunct : pushable_conjuncts)
            mapped_pushable_conjuncts.push_back(mapper.map(conjunct));

        push_filter = PredicateUtils::combineConjuncts(mapped_pushable_conjuncts);
    }

    // push filter into storage
    if (!PredicateUtils::isTruePredicate(push_filter))
        push_filter = applyFilter(table_step, push_filter, context, storage_statistics);

    // construnct the remaing filter
    auto mapper = SymbolMapper::simpleMapper(column_to_alias);
    non_pushable_conjuncts.push_back(mapper.map(push_filter));
    return PredicateUtils::combineConjuncts(non_pushable_conjuncts);
}

ConstRefPatternPtr PushLimitIntoTableScan::getPattern() const
{
    static auto pattern = Patterns::limit()
        .matchingStep<LimitStepExt>([](auto const & limit_step) { return !limit_step.isAlwaysReadTillEnd(); })
        .withSingle(Patterns::tableScan())
        .result();
    return pattern;
}

TransformResult PushLimitIntoTableScan::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    const auto * limit_step = dynamic_cast<const LimitStepExt *>(node->getStep().get());
    auto table_scan = node->getChildren()[0];

    if (limit_step->hasPreparedParam())
        return {};

    auto copy_table_step = QueryPlanStepHelper::copyQueryPlanStep(table_scan->getStep(), rule_context.context);

    auto table_step = dynamic_cast<TableScanStepExt *>(copy_table_step.get());
    bool applied = table_step->setLimit(limit_step->getLimit() + limit_step->getOffset(), rule_context.context);
    if (!applied)
        return {}; // repeat calls

    table_scan->setStep(copy_table_step);
    node->replaceChildren({table_scan});
    return node;
}


ConstRefPatternPtr PushAggregationIntoTableScan::getPattern() const
{
    static auto pattern = Patterns::aggregating()
        .matchingStep<AggregatingStepExt>([](auto & step) { return !step.isGroupingSet(); })
        .withSingle(Patterns::tableScan())
        .result();
    return pattern;
}

TransformResult PushAggregationIntoTableScan::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    if (!isOptimizerProjectionSupportEnabled(rule_context))
        return {};

    auto copy_step = QueryPlanStepHelper::copyQueryPlanStep(node->getChildren()[0]->getStep(), (rule_context.context));
    auto *copy_table_step = dynamic_cast<TableScanStepExt *>(copy_step.get());

    // TODO: combine aggregates if grouping keys are the same
    chassert(copy_table_step != nullptr);
    // coverity[var_deref_model]
    if (copy_table_step->getPushdownAggregation())
        return {};

    copy_table_step->setPushdownAggregation(QueryPlanStepHelper::copyQueryPlanStep(node->getStep(), rule_context.context));
    copy_table_step->formatOutputStream(rule_context.context);
    return PlanNodeBase::createPlanNode(rule_context.context->getOptimizerContext()->nextNodeId(), std::move(copy_step), {}, node->getStatistics());
}

ConstRefPatternPtr PushProjectionIntoTableScan::getPattern() const
{
    static auto pattern = Patterns::project()
        .withSingle(Patterns::tableScan().matchingStep<TableScanStepExt>([](const auto & step) { return !step.getPushdownAggregation(); }))
        .result();
    return pattern;
}

TransformResult PushProjectionIntoTableScan::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    if (!isOptimizerProjectionSupportEnabled(rule_context))
        return {};

    auto copy_step = QueryPlanStepHelper::copyQueryPlanStep(node->getChildren()[0]->getStep(), rule_context.context);
    auto *copy_table_step = dynamic_cast<TableScanStepExt *>(copy_step.get());

    // TODO: inline projection
    chassert(copy_table_step != nullptr);
    // coverity[var_deref_model]
    if (copy_table_step->getPushdownProjection())
        return {};

    copy_table_step->setPushdownProjection(QueryPlanStepHelper::copyQueryPlanStep(node->getStep(), rule_context.context));
    copy_table_step->formatOutputStream(rule_context.context);
    return PlanNodeBase::createPlanNode(rule_context.context->getOptimizerContext()->nextNodeId(), std::move(copy_step), {}, node->getStatistics());
}

ConstRefPatternPtr PushFilterIntoTableScan::getPattern() const
{
    static auto pattern = Patterns::filter().withSingle(
               Patterns::tableScan().matchingStep<TableScanStepExt>(
                   [](const auto & step) { return !step.getPushdownProjection() && !step.getPushdownAggregation(); })).result();
    return pattern;
}

TransformResult PushFilterIntoTableScan::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    if (!isOptimizerProjectionSupportEnabled(rule_context))
        return {};

    auto copy_step =  QueryPlanStepHelper::copyQueryPlanStep(node->getChildren()[0]->getStep(), rule_context.context);
    auto *copy_table_step = dynamic_cast<TableScanStepExt *>(copy_step.get());
    chassert(copy_table_step != nullptr);

    // in case of TableScan has already a pushdown filter, combine them into one
    if (const auto * pushdown_filter_step = copy_table_step->getPushdownFilterCast())
    {
        const auto & old_pushdown_filter = pushdown_filter_step->getFilter();
        const auto & filter_step_filter = dynamic_cast<const FilterStepExt *>(node->getStep().get())->getFilter();

        auto new_pushdown_filter = PredicateUtils::combineConjuncts(ConstASTs{old_pushdown_filter, filter_step_filter});
        copy_table_step->setPushdownFilter(std::make_shared<FilterStepExt>(pushdown_filter_step->getInputStreams()[0], new_pushdown_filter));
    }
    else
    {
        copy_table_step->setPushdownFilter(QueryPlanStepHelper::copyQueryPlanStep(node->getStep(), rule_context.context));
    }

    // coverity[var_deref_model]
    copy_table_step->formatOutputStream(rule_context.context);


    return PlanNodeBase::createPlanNode(rule_context.context->getOptimizerContext()->nextNodeId(), std::move(copy_step), {}, node->getStatistics());
}

ConstRefPatternPtr PushIndexProjectionIntoTableScan::getPattern() const
{
    static auto pattern = Patterns::project().withSingle(
               Patterns::tableScan().matchingStep<TableScanStepExt>(
                   [](const auto & step) { return !step.getPushdownAggregation() ; })).result();
    return pattern;
}

TransformResult PushIndexProjectionIntoTableScan::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    if (!isOptimizerIndexProjectionSupportEnabled(rule_context))
        return {};

    auto * projection_step = dynamic_cast<ProjectionStepExt *>(node->getStep().get());

    if (projection_step && !projection_step->isIndexProject())
        return {};

    auto copy_step = QueryPlanStepHelper::copyQueryPlanStep(node->getChildren()[0]->getStep(), rule_context.context);
    auto * copy_table_step = dynamic_cast<TableScanStepExt *>(copy_step.get());

    if (!dynamic_cast<StorageDistributed *>(copy_table_step->getStorage().get()))
        return {};

    if (copy_table_step->hasInlineExpressions())
        return {};

    const auto & all_name_to_type = projection_step->getNameToType();

    // split node into two projection a, b.
    // a contains all original assignments, but convert function to identifier about `arraysetcheck`.
    // b contains all output symbols, and `arraysetcheck` with function type.
    // we push b into table_scan, then return a->(table_scan).
    Assignments a_assignments;
    NameToType a_name_to_type;
    Assignments  b_assignments;
    // NameToType b_name_to_type;
    bool projection_a_no_need = true;
    for (const auto & assignment : projection_step->getAssignments())
    {
        //todo: liyang453, storage: need functionCanUseBitmapIndex in Storages/MergeTree/Index/BitmapIndexHelper.h
        /*
        if (const auto * func = assignment.second->as<ASTFunction>())
        {
            if (functionCanUseBitmapIndex(*func))
            {
                b_assignments.emplace_back(assignment);
                a_assignments.emplace_back(assignment.first, std::make_shared<ASTIdentifier>(assignment.first));
                a_name_to_type.emplace(assignment.first, all_name_to_type.at(assignment.first));
                continue;
            }
            projection_a_no_need = false;
        }
            */
        a_assignments.emplace_back(assignment);
        a_name_to_type.emplace(assignment.first, all_name_to_type.at(assignment.first));
    }

    if (b_assignments.empty())
        return {};

    // prune simple expressions & map symbols
    auto prepare_index_projection = [&](const Assignments & assignments) {
        Assignments new_assignments;
        auto aliases_to_columns = copy_table_step->getAliasToColumnMap();
        SymbolMapper mapper = SymbolMapper::simpleMapper(aliases_to_columns);

        for (const auto & ass : assignments)
            if (!ass.second->as<ASTIdentifier>())
                new_assignments.emplace_back(ass.first, mapper.map(ass.second));

        return new_assignments;
    };

    if (projection_a_no_need)
    {
        copy_table_step->setInlineExpressions(prepare_index_projection(projection_step->getAssignments()), rule_context.context);
        return PlanNodeBase::createPlanNode(rule_context.context->getOptimizerContext()->nextNodeId(), std::move(copy_step), {}, node->getStatistics());
    }

    copy_table_step->setInlineExpressions(prepare_index_projection(b_assignments), rule_context.context);

    auto table_scan_node = PlanNodeBase::createPlanNode(rule_context.context->getOptimizerContext()->nextNodeId(), std::move(copy_step), {}, node->getStatistics());
    auto a_projection_step = std::make_shared<ProjectionStepExt>(table_scan_node->getCurrentDataStream(), a_assignments, a_name_to_type);
    return ProjectionStepExtNode::createPlanNode(rule_context.context->getOptimizerContext()->nextNodeId(), std::move(a_projection_step), {table_scan_node}, node->getStatistics());
}

}
