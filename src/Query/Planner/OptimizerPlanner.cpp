#include <Query/Planner/OptimizerPlanner.h>

#include <Common/ProfileEvents.h>

#include <Analyzer/UnionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Analyzer/WindowFunctionsUtils.h>
#include <Analyzer/AggregationUtils.h>
#include <Analyzer/ConstantNode.h>

#include <Parsers/parseQuery.h>

#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>

#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>

#include <Processors/Sources/NullSource.h>

#include <Planner/Utils.h>
#include <Planner/CollectColumnIdentifiers.h>
#include <Planner/findQueryForParallelReplicas.h>
#include <Planner/CollectSets.h>
#include <Planner/CollectTableExpressionData.h>
#include <Planner/PlannerJoinTree.h>

#include <Query/Planner/PlannerContextExt.h>
#include <Query/Processors/QueryPlan/FilterStepExt.h>
#include <Query/Processors/QueryPlan/TableScanStepExt.h>

#include <Storages/StorageDummy.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageMerge.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace ProfileEvents
{
    extern const Event SelectQueriesWithSubqueries;
    extern const Event QueriesWithSubqueries;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int TOO_DEEP_SUBQUERIES;
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int ACCESS_DENIED;
    extern const int TOO_MANY_COLUMNS;
}


UInt64 mainQueryNodeBlockSizeByLimit(const SelectQueryInfo & select_query_info)
{
    auto const & main_query_node = select_query_info.query_tree->as<QueryNode const &>();

    /// Constness of limit and offset is validated during query analysis stage
    size_t limit_length = 0;
    if (main_query_node.hasLimit())
        limit_length = main_query_node.getLimit()->as<ConstantNode &>().getValue().safeGet<UInt64>();

    size_t limit_offset = 0;
    if (main_query_node.hasOffset())
        limit_offset = main_query_node.getOffset()->as<ConstantNode &>().getValue().safeGet<UInt64>();

    /** If not specified DISTINCT, WHERE, GROUP BY, HAVING, ORDER BY, JOIN, LIMIT BY, LIMIT WITH TIES
      * but LIMIT is specified, and limit + offset < max_block_size,
      * then as the block size we will use limit + offset (not to read more from the table than requested),
      * and also set the number of threads to 1.
      */
    if (main_query_node.hasLimit()
        && !main_query_node.isDistinct()
        && !main_query_node.isLimitWithTies()
        && !main_query_node.hasPrewhere()
        && !main_query_node.hasWhere()
        && select_query_info.filter_asts.empty()
        && !main_query_node.hasGroupBy()
        && !main_query_node.hasHaving()
        && !main_query_node.hasOrderBy()
        && !main_query_node.hasLimitBy()
        && !select_query_info.need_aggregate
        && !select_query_info.has_window
        && limit_length <= std::numeric_limits<UInt64>::max() - limit_offset)
        return limit_length + limit_offset;
    return 0;
}

std::unique_ptr<ExpressionStep> createComputeAliasColumnsStep(
    const std::unordered_map<std::string, ActionsDAGPtr> & alias_column_expressions, const DataStream & current_data_stream)
{
    ActionsDAGPtr merged_alias_columns_actions_dag = std::make_shared<ActionsDAG>(current_data_stream.header.getColumnsWithTypeAndName());
    ActionsDAG::NodeRawConstPtrs action_dag_outputs = merged_alias_columns_actions_dag->getInputs();

    for (const auto & [column_name, alias_column_actions_dag] : alias_column_expressions)
    {
        const auto & current_outputs = alias_column_actions_dag->getOutputs();
        action_dag_outputs.insert(action_dag_outputs.end(), current_outputs.begin(), current_outputs.end());
        merged_alias_columns_actions_dag->mergeNodes(std::move(*alias_column_actions_dag));
    }

    for (const auto * output_node : action_dag_outputs)
        merged_alias_columns_actions_dag->addOrReplaceInOutputs(*output_node);
    merged_alias_columns_actions_dag->removeUnusedActions(false);

    auto alias_column_step = std::make_unique<ExpressionStep>(current_data_stream, std::move(merged_alias_columns_actions_dag));
    alias_column_step->setStepDescription("Compute alias columns");
    return alias_column_step;
}


FilterDAGInfo buildRowPolicyFilterIfNeeded(const StoragePtr & storage,
    SelectQueryInfo & table_expression_query_info,
    PlannerContextPtr & planner_context,
    std::set<std::string> & used_row_policies)
{
    auto storage_id = storage->getStorageID();
    const auto & query_context = planner_context->getQueryContext();

    auto row_policy_filter = query_context->getRowPolicyFilter(storage_id.getDatabaseName(), storage_id.getTableName(), RowPolicyFilterType::SELECT_FILTER);
    if (!row_policy_filter || row_policy_filter->empty())
        return {};

    for (const auto & row_policy : row_policy_filter->policies)
    {
        auto name = row_policy->getFullName().toString();
        used_row_policies.emplace(std::move(name));
    }

    return buildFilterInfo(row_policy_filter->expression, table_expression_query_info.table_expression, planner_context);
}


/// Apply filters from additional_table_filters setting
FilterDAGInfo buildAdditionalFiltersIfNeeded(const StoragePtr & storage,
    const String & table_expression_alias,
    SelectQueryInfo & table_expression_query_info,
    PlannerContextPtr & planner_context)
{
    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    auto const & additional_filters = settings.additional_table_filters.value;
    if (additional_filters.empty())
        return {};

    auto const & storage_id = storage->getStorageID();

    ASTPtr additional_filter_ast;
    for (const auto & additional_filter : additional_filters)
    {
        const auto & tuple = additional_filter.safeGet<const Tuple &>();
        auto const & table = tuple.at(0).safeGet<String>();
        auto const & filter = tuple.at(1).safeGet<String>();

        if (table == table_expression_alias ||
            (table == storage_id.getTableName() && query_context->getCurrentDatabase() == storage_id.getDatabaseName()) ||
            (table == storage_id.getFullNameNotQuoted()))
        {
            ParserExpression parser;
            additional_filter_ast = parseQuery(
                parser, filter.data(), filter.data() + filter.size(),
                "additional filter", settings.max_query_size, settings.max_parser_depth, settings.max_parser_backtracks);
            break;
        }
    }

    if (!additional_filter_ast)
        return {};

    table_expression_query_info.additional_filter_ast = additional_filter_ast;
    return buildFilterInfo(additional_filter_ast, table_expression_query_info.table_expression, planner_context);
}


JoinTreeQueryPlan buildQueryPlanForTableExpression(QueryTreeNodePtr table_expression,
    const SelectQueryInfo & select_query_info,
    const SelectQueryOptions & select_query_options,
    PlannerContextPtr & planner_context,
    bool is_single_table_expression)
{
    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    auto & table_expression_data = planner_context->getTableExpressionDataOrThrow(table_expression);

    QueryProcessingStage::Enum from_stage = QueryProcessingStage::Enum::FetchColumns;

    QueryPlan query_plan;
    std::unordered_map<const QueryNode *, const QueryPlan::Node *> query_node_to_plan_step_mapping;
    std::set<std::string> used_row_policies;

    auto * table_node = table_expression->as<TableNode>();

    if (!table_node)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Table expression only support table now.");
    }

    if (table_node)
    {
        const auto & storage = table_node->getStorage();
        const auto & storage_snapshot = table_node->getStorageSnapshot();

        auto table_expression_query_info = select_query_info;
        table_expression_query_info.table_expression = table_expression;
        table_expression_query_info.filter_actions_dag = table_expression_data.getFilterActions();
        table_expression_query_info.optimized_prewhere_info = table_expression_data.getPrewhereInfo();
        table_expression_query_info.analyzer_can_use_parallel_replicas_on_follower = table_node == planner_context->getGlobalPlannerContext()->parallel_replicas_table;

        size_t max_streams = settings.max_threads;
        size_t max_threads_execute_query = settings.max_threads;

        /**
         * To simultaneously query more remote servers when async_socket_for_remote is off
         * instead of max_threads, max_distributed_connections is used:
         * since threads there mostly spend time waiting for data from remote servers,
         * we can increase the degree of parallelism to avoid sequential querying of remote servers.
         *
         * DANGER: that can lead to insane number of threads working if there are a lot of stream and prefer_localhost_replica is used.
         *
         * That is not needed when async_socket_for_remote is on, because in that case
         * threads are not blocked waiting for data from remote servers.
         *
         */
        bool is_sync_remote = table_expression_data.isRemote() && !settings.async_socket_for_remote;
        if (is_sync_remote)
        {
            max_streams = settings.max_distributed_connections;
            max_threads_execute_query = settings.max_distributed_connections;
        }

        UInt64 max_block_size = settings.max_block_size;
        UInt64 max_block_size_limited = 0;
        if (is_single_table_expression)
        {
            /** If not specified DISTINCT, WHERE, GROUP BY, HAVING, ORDER BY, JOIN, LIMIT BY, LIMIT WITH TIES
              * but LIMIT is specified, and limit + offset < max_block_size,
              * then as the block size we will use limit + offset (not to read more from the table than requested),
              * and also set the number of threads to 1.
              */
            max_block_size_limited = mainQueryNodeBlockSizeByLimit(select_query_info);
            if (max_block_size_limited)
            {
                if (max_block_size_limited < max_block_size)
                {
                    max_block_size = std::max<UInt64>(1, max_block_size_limited);
                    max_streams = 1;
                    max_threads_execute_query = 1;
                }

                if (select_query_info.local_storage_limits.local_limits.size_limits.max_rows != 0)
                {
                    if (max_block_size_limited < select_query_info.local_storage_limits.local_limits.size_limits.max_rows)
                        table_expression_query_info.limit = max_block_size_limited;
                }
                else
                {
                    table_expression_query_info.limit = max_block_size_limited;
                }
            }

            if (!max_block_size)
                throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND,
                    "Setting 'max_block_size' cannot be zero");
        }

        if (max_streams == 0)
            max_streams = 1;

        /// If necessary, we request more sources than the number of threads - to distribute the work evenly over the threads
        if (max_streams > 1 && !is_sync_remote)
            max_streams = static_cast<size_t>(max_streams * settings.max_streams_to_max_threads_ratio);

        table_expression_query_info.table_expression_modifiers = table_node->getTableExpressionModifiers();


        bool need_rewrite_query_with_final = storage->needRewriteQueryWithFinal(table_expression_data.getColumnNames());
        if (need_rewrite_query_with_final)
        {
            if (table_expression_query_info.table_expression_modifiers)
            {
                const auto & table_expression_modifiers = table_expression_query_info.table_expression_modifiers;
                auto sample_size_ratio = table_expression_modifiers->getSampleSizeRatio();
                auto sample_offset_ratio = table_expression_modifiers->getSampleOffsetRatio();

                table_expression_query_info.table_expression_modifiers = TableExpressionModifiers(true /*has_final*/,
                    sample_size_ratio,
                    sample_offset_ratio);
            }
            else
            {
                table_expression_query_info.table_expression_modifiers = TableExpressionModifiers(true /*has_final*/,
                    {} /*sample_size_ratio*/,
                    {} /*sample_offset_ratio*/);
            }
        }

        /// Apply trivial_count optimization if possible
        bool is_trivial_count_applied = !select_query_options.only_analyze && is_single_table_expression
            && table_node && select_query_info.has_aggregates && settings.additional_table_filters.value.empty();

        if (is_trivial_count_applied)
        {
            from_stage = QueryProcessingStage::WithMergeableState;
        }
        else
        {
            if (!select_query_options.only_analyze)
            {
                auto storage_merge_tree = std::dynamic_pointer_cast<MergeTreeData>(storage);
                if (storage_merge_tree && query_context->canUseParallelReplicasOnInitiator()
                    && settings.parallel_replicas_min_number_of_rows_per_replica > 0)
                {
                    UInt64 rows_to_read
                        = storage_merge_tree->estimateNumberOfRowsToRead(query_context, storage_snapshot, table_expression_query_info);

                    if (max_block_size_limited && (max_block_size_limited < rows_to_read))
                        rows_to_read = max_block_size_limited;

                    size_t number_of_replicas_to_use = rows_to_read / settings.parallel_replicas_min_number_of_rows_per_replica;
                    LOG_TRACE(
                        getLogger("Planner"),
                        "Estimated {} rows to read. It is enough work for {} parallel replicas",
                        rows_to_read,
                        number_of_replicas_to_use);

                    if (number_of_replicas_to_use <= 1)
                    {
                        planner_context->getMutableQueryContext()->setSetting(
                            "allow_experimental_parallel_reading_from_replicas", Field(0));
                        planner_context->getMutableQueryContext()->setSetting("max_parallel_replicas", Field(0));
                        LOG_DEBUG(getLogger("Planner"), "Disabling parallel replicas because there aren't enough rows to read");
                    }
                    else if (number_of_replicas_to_use < settings.max_parallel_replicas)
                    {
                        planner_context->getMutableQueryContext()->setSetting("max_parallel_replicas", number_of_replicas_to_use);
                        LOG_DEBUG(getLogger("Planner"), "Reducing the number of replicas to use to {}", number_of_replicas_to_use);
                    }
                }

                auto & prewhere_info = table_expression_query_info.prewhere_info;
                const auto & prewhere_actions = table_expression_data.getPrewhereFilterActions();

                if (prewhere_actions)
                {
                    prewhere_info = std::make_shared<PrewhereInfo>();
                    prewhere_info->prewhere_actions = prewhere_actions;
                    prewhere_info->prewhere_column_name = prewhere_actions->getOutputs().at(0)->result_name;
                    prewhere_info->remove_prewhere_column = true;
                    prewhere_info->need_filter = true;
                }

                // updatePrewhereOutputsIfNeeded(table_expression_query_info, table_expression_data.getColumnNames(), storage_snapshot);

                const auto & columns_names = table_expression_data.getColumnNames();

                std::vector<std::pair<FilterDAGInfo, std::string>> where_filters;
                const auto add_filter = [&](const FilterDAGInfo & filter_info, std::string description)
                {
                    if (!filter_info.actions)
                        return;

                    bool is_final = table_expression_query_info.table_expression_modifiers
                        && table_expression_query_info.table_expression_modifiers->hasFinal();
                    bool optimize_move_to_prewhere
                        = settings.optimize_move_to_prewhere && (!is_final || settings.optimize_move_to_prewhere_if_final);

                    auto supported_prewhere_columns = storage->supportedPrewhereColumns();
                    if (storage->canMoveConditionsToPrewhere() && optimize_move_to_prewhere && (!supported_prewhere_columns || supported_prewhere_columns->contains(filter_info.column_name)))
                    {
                        if (!prewhere_info)
                            prewhere_info = std::make_shared<PrewhereInfo>();

                        if (!prewhere_info->prewhere_actions)
                        {
                            prewhere_info->prewhere_actions = filter_info.actions;
                            prewhere_info->prewhere_column_name = filter_info.column_name;
                            prewhere_info->remove_prewhere_column = filter_info.do_remove_column;
                            prewhere_info->need_filter = true;
                        }
                        else if (!prewhere_info->row_level_filter)
                        {
                            prewhere_info->row_level_filter = filter_info.actions;
                            prewhere_info->row_level_column_name = filter_info.column_name;
                            prewhere_info->need_filter = true;
                        }
                        else
                        {
                            where_filters.emplace_back(filter_info, std::move(description));
                        }

                    }
                    else
                    {
                        where_filters.emplace_back(filter_info, std::move(description));
                    }
                };

                auto row_policy_filter_info
                    = buildRowPolicyFilterIfNeeded(storage, table_expression_query_info, planner_context, used_row_policies);
                add_filter(row_policy_filter_info, "Row-level security filter");
                if (row_policy_filter_info.actions)
                    table_expression_data.setRowLevelFilterActions(row_policy_filter_info.actions);

                const auto & table_expression_alias = table_expression->getOriginalAlias();
                auto additional_filters_info
                    = buildAdditionalFiltersIfNeeded(storage, table_expression_alias, table_expression_query_info, planner_context);
                add_filter(additional_filters_info, "additional filter");

                from_stage = storage->getQueryProcessingStage(
                    query_context, select_query_options.to_stage, storage_snapshot, table_expression_query_info);

                // Todo just use columns_names for TableScanStepExt
                NamesWithAliases columns_with_aliases;
                columns_with_aliases.reserve(columns_names.size());
                for (const auto & column : columns_names)
                {
                    columns_with_aliases.emplace_back(column, column);
                }

                auto table_step = std::make_shared<TableScanStepExt>(planner_context->getQueryContext(), storage->getStorageID(), columns_with_aliases, table_expression_query_info, max_block_size, table_expression->getOriginalAlias());
                query_plan.addStep(std::move(table_step));

                const auto & alias_column_expressions = table_expression_data.getAliasColumnExpressions();
                if (!alias_column_expressions.empty() && query_plan.isInitialized() && from_stage == QueryProcessingStage::FetchColumns)
                {
                    auto alias_column_step = createComputeAliasColumnsStep(alias_column_expressions, query_plan.getCurrentDataStream());
                    query_plan.addStep(std::move(alias_column_step));
                }

                for (const auto & filter_info_and_description : where_filters)
                {
                    const auto & [filter_info, description] = filter_info_and_description;
                    if (query_plan.isInitialized() &&
                        from_stage == QueryProcessingStage::FetchColumns &&
                        filter_info.actions)
                    {
                        auto filter_step = std::make_unique<FilterStep>(query_plan.getCurrentDataStream(),
                            filter_info.actions,
                            filter_info.column_name,
                            filter_info.do_remove_column);
                        filter_step->setStepDescription(description);
                        query_plan.addStep(std::move(filter_step));
                    }
                }

                if (query_context->hasQueryContext() && !select_query_options.is_internal)
                {
                    auto local_storage_id = storage->getStorageID();
                    query_context->getQueryContext()->addQueryAccessInfo(
                        backQuoteIfNeed(local_storage_id.getDatabaseName()),
                        local_storage_id.getFullTableName(),
                        columns_names);
                }
            }

            if (query_plan.isInitialized())
            {
                /** Specify the number of threads only if it wasn't specified in storage.
                  *
                  * But in case of remote query and prefer_localhost_replica=1 (default)
                  * The inner local query (that is done in the same process, without
                  * network interaction), it will setMaxThreads earlier and distributed
                  * query will not update it.
                  */
                if (!query_plan.getMaxThreads() || is_sync_remote)
                    query_plan.setMaxThreads(max_threads_execute_query);

                query_plan.setConcurrencyControl(settings.use_concurrency_control);
            }
            else
            {
                /// Create step which reads from empty source if storage has no data.
                const auto & column_names = table_expression_data.getSelectedColumnsNames();
                auto source_header = storage_snapshot->getSampleBlockForColumns(column_names);
                Pipe pipe(std::make_shared<NullSource>(source_header));
                auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
                read_from_pipe->setStepDescription("Read from NullSource");
                query_plan.addStep(std::move(read_from_pipe));
            }
        }
    }

    return JoinTreeQueryPlan{
        .query_plan = std::move(query_plan),
        .from_stage = from_stage,
        .used_row_policies = std::move(used_row_policies),
        .query_node_to_plan_step_mapping = std::move(query_node_to_plan_step_mapping),
    };
}


/** Storages can rely that filters that for storage will be available for analysis before
  * getQueryProcessingStage method will be called.
  *
  * StorageDistributed skip unused shards optimization relies on this.
  * Parallel replicas estimation relies on this too.
  * StorageMerge common header calculation relies on this too.
  *
  * To collect filters that will be applied to specific table in case we have JOINs requires
  * to run query plan optimization pipeline.
  *
  * Algorithm:
  * 1. Replace all table expressions in query tree with dummy tables.
  * 2. Build query plan.
  * 3. Optimize query plan.
  * 4. Extract filters from ReadFromDummy query plan steps from query plan leaf nodes.
  */

FiltersForTableExpressionMap collectFiltersForAnalysis(const QueryTreeNodePtr & query_tree, const QueryTreeNodes & table_nodes, const ContextPtr & query_context)
{
    bool collect_filters = false;
    const auto & settings = query_context->getSettingsRef();

    bool parallel_replicas_estimation_enabled
        = query_context->canUseParallelReplicasOnInitiator() && settings.parallel_replicas_min_number_of_rows_per_replica > 0;

    for (const auto & table_expression : table_nodes)
    {
        auto * table_node = table_expression->as<TableNode>();
        auto * table_function_node = table_expression->as<TableFunctionNode>();
        if (!table_node && !table_function_node)
            continue;

        const auto & storage = table_node ? table_node->getStorage() : table_function_node->getStorage();
        if (typeid_cast<const StorageDistributed *>(storage.get()) || typeid_cast<const StorageMerge *>(storage.get())
            || (parallel_replicas_estimation_enabled && std::dynamic_pointer_cast<MergeTreeData>(storage)))
        {
            collect_filters = true;
            break;
        }
    }

    if (!collect_filters)
        return {};

    ResultReplacementMap replacement_map;

    auto updated_query_tree = replaceTableExpressionsWithDummyTables(query_tree, table_nodes, query_context, &replacement_map);

    std::unordered_map<const IStorage *, QueryTreeNodePtr> dummy_storage_to_table;

    for (auto & [from_table_expression, dummy_table_expression] : replacement_map)
    {
        auto * dummy_storage = dummy_table_expression->as<TableNode &>().getStorage().get();
        dummy_storage_to_table.emplace(dummy_storage, from_table_expression);
    }

    SelectQueryOptions select_query_options;
    OptimizerPlanner planner(updated_query_tree, select_query_options);
    planner.buildQueryPlanIfNeeded();

    auto & result_query_plan = planner.getQueryPlan();

    auto optimization_settings = QueryPlanOptimizationSettings::fromContext(query_context);
    result_query_plan.optimize(optimization_settings);

    FiltersForTableExpressionMap res;

    std::vector<QueryPlan::Node *> nodes_to_process;
    nodes_to_process.push_back(result_query_plan.getRootNode());

    while (!nodes_to_process.empty())
    {
        const auto * node_to_process = nodes_to_process.back();
        nodes_to_process.pop_back();
        nodes_to_process.insert(nodes_to_process.end(), node_to_process->children.begin(), node_to_process->children.end());

        auto * read_from_dummy = typeid_cast<ReadFromDummy *>(node_to_process->step.get());
        if (!read_from_dummy)
            continue;

        auto filter_actions = read_from_dummy->getFilterActionsDAG();
        const auto & table_node = dummy_storage_to_table.at(&read_from_dummy->getStorage());
        res[table_node] = FiltersForTableExpression{std::move(filter_actions), read_from_dummy->getPrewhereInfo()};
    }

    return res;
}

FiltersForTableExpressionMap collectFiltersForAnalysis(const QueryTreeNodePtr & query_tree_node, SelectQueryOptions & select_query_options)
{
    if (select_query_options.only_analyze)
        return {};

    auto * query_node = query_tree_node->as<QueryNode>();
    auto * union_node = query_tree_node->as<UnionNode>();

    if (!query_node && !union_node)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Expected QUERY or UNION node. Actual {}",
            query_tree_node->formatASTForErrorMessage());

    auto context = query_node ? query_node->getContext() : union_node->getContext();

    auto table_expressions_nodes
        = extractTableExpressions(query_tree_node, false /* add_array_join */, true /* recursive */);

    return collectFiltersForAnalysis(query_tree_node, table_expressions_nodes, context);
}


PlannerContextExtPtr buildPlannerContext(const QueryTreeNodePtr & query_tree_node,
    const SelectQueryOptions & select_query_options,
    GlobalPlannerContextPtr global_planner_context)
{
    auto * query_node = query_tree_node->as<QueryNode>();
    auto * union_node = query_tree_node->as<UnionNode>();

    if (!query_node && !union_node)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Expected QUERY or UNION node. Actual {}",
            query_tree_node->formatASTForErrorMessage());

    auto & mutable_context = query_node ? query_node->getMutableContext() : union_node->getMutableContext();
    size_t max_subquery_depth = mutable_context->getSettingsRef().max_subquery_depth;
    if (max_subquery_depth && select_query_options.subquery_depth > max_subquery_depth)
        throw Exception(ErrorCodes::TOO_DEEP_SUBQUERIES,
            "Too deep subqueries. Maximum: {}",
            max_subquery_depth);

    const auto & client_info = mutable_context->getClientInfo();
    auto min_major = static_cast<UInt64>(DBMS_MIN_MAJOR_VERSION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD);
    auto min_minor = static_cast<UInt64>(DBMS_MIN_MINOR_VERSION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD);

    bool need_to_disable_two_level_aggregation = client_info.query_kind == ClientInfo::QueryKind::SECONDARY_QUERY &&
        client_info.connection_client_version_major < min_major &&
        client_info.connection_client_version_minor < min_minor;

    if (need_to_disable_two_level_aggregation)
    {
        /// Disable two-level aggregation due to version incompatibility
        mutable_context->setSetting("group_by_two_level_threshold", Field(0));
        mutable_context->setSetting("group_by_two_level_threshold_bytes", Field(0));
    }

    if (select_query_options.is_subquery)
        updateContextForSubqueryExecution(mutable_context);

    return std::make_shared<PlannerContextExt>(mutable_context, std::move(global_planner_context), select_query_options);
}

OptimizerPlanner::OptimizerPlanner(const QueryTreeNodePtr & query_tree_,
    SelectQueryOptions & select_query_options_)
    : query_tree(query_tree_)
    , select_query_options(select_query_options_)
    , planner_context(buildPlannerContext(query_tree, select_query_options,
        std::make_shared<GlobalPlannerContext>(
            findQueryForParallelReplicas(query_tree, select_query_options),
            findTableForParallelReplicas(query_tree, select_query_options),
            collectFiltersForAnalysis(query_tree, select_query_options))))
{
}

/// Extend lifetime of query context, storages, and table locks
void extendQueryContextAndStoragesLifetime(QueryPlan & query_plan, const PlannerContextExtPtr & planner_context)
{
    query_plan.addInterpreterContext(planner_context->getQueryContext());

    for (const auto & [table_expression, _] : planner_context->getPlannerContextPtr()->getTableExpressionNodeToData())
    {
        if (auto * table_node = table_expression->as<TableNode>())
        {
            query_plan.addStorageHolder(table_node->getStorage());
            query_plan.addTableLock(table_node->getStorageLock());
        }
        else if (auto * table_function_node = table_expression->as<TableFunctionNode>())
        {
            query_plan.addStorageHolder(table_function_node->getStorage());
        }
    }
}

void OptimizerPlanner::buildQueryPlanIfNeeded()
{
    if (query_plan.isInitialized())
        return;

    LOG_TRACE(getLogger("Planner"), "Query {} to stage {}{}",
        query_tree->formatConvertedASTForErrorMessage(),
        QueryProcessingStage::toString(select_query_options.to_stage),
        select_query_options.only_analyze ? " only analyze" : "");

    if (query_tree->getNodeType() == QueryTreeNodeType::UNION)
        buildPlanForUnionNode();
    else
        buildPlanForQueryNode();

    extendQueryContextAndStoragesLifetime(query_plan, planner_context);
}

SelectQueryInfo OptimizerPlanner::buildSelectQueryInfo() const
{
    return ::DB::buildSelectQueryInfo(query_tree, planner_context->getPlannerContextPtr());
}


bool shouldIgnoreQuotaAndLimits(const TableNode & table_node)
{
    const auto & storage_id = table_node.getStorageID();
    if (!storage_id.hasDatabase())
        return false;
    if (storage_id.database_name == DatabaseCatalog::SYSTEM_DATABASE)
    {
        static const boost::container::flat_set<std::string_view> tables_ignoring_quota{"quotas", "quota_limits", "quota_usage", "quotas_usage", "one"};
        if (tables_ignoring_quota.contains(storage_id.table_name))
            return true;
    }
    return false;
}


NameSet checkAccessRights(const TableNode & table_node, const Names & column_names, const ContextPtr & query_context)
{
    /// StorageDummy is created on preliminary stage, ignore access check for it.
    if (typeid_cast<const StorageDummy *>(table_node.getStorage().get()))
        return {};

    const auto & storage_id = table_node.getStorageID();
    const auto & storage_snapshot = table_node.getStorageSnapshot();

    if (column_names.empty())
    {
        NameSet accessible_columns;
        /** For a trivial queries like "SELECT count() FROM table", "SELECT 1 FROM table" access is granted if at least
          * one table column is accessible.
          */
        auto access = query_context->getAccess();
        for (const auto & column : storage_snapshot->metadata->getColumns())
        {
            if (access->isGranted(AccessType::SELECT, storage_id.database_name, storage_id.table_name, column.name))
                accessible_columns.insert(column.name);
        }

        if (accessible_columns.empty())
        {
            throw Exception(ErrorCodes::ACCESS_DENIED,
                "{}: Not enough privileges. To execute this query, it's necessary to have the grant SELECT for at least one column on {}",
                query_context->getUserName(),
                storage_id.getFullTableName());
        }
        return accessible_columns;
    }

    // In case of cross-replication we don't know what database is used for the table.
    // `storage_id.hasDatabase()` can return false only on the initiator node.
    // Each shard will use the default database (in the case of cross-replication shards may have different defaults).
    if (storage_id.hasDatabase())
        query_context->checkAccess(AccessType::SELECT, storage_id, column_names);

    return {};
}

NameAndTypePair chooseSmallestColumnToReadFromStorage(const StoragePtr & storage, const StorageSnapshotPtr & storage_snapshot, const NameSet & column_names_allowed_to_select)
{
    /** We need to read at least one column to find the number of rows.
      * We will find a column with minimum <compressed_size, type_size, uncompressed_size>.
      * Because it is the column that is cheapest to read.
      */
    class ColumnWithSize
    {
    public:
        ColumnWithSize(NameAndTypePair column_, ColumnSize column_size_)
            : column(std::move(column_))
            , compressed_size(column_size_.data_compressed)
            , uncompressed_size(column_size_.data_uncompressed)
            , type_size(column.type->haveMaximumSizeOfValue() ? column.type->getMaximumSizeOfValueInMemory() : 100)
        {
        }

        bool operator<(const ColumnWithSize & rhs) const
        {
            return std::tie(compressed_size, type_size, uncompressed_size)
                < std::tie(rhs.compressed_size, rhs.type_size, rhs.uncompressed_size);
        }

        NameAndTypePair column;
        size_t compressed_size = 0;
        size_t uncompressed_size = 0;
        size_t type_size = 0;
    };

    std::vector<ColumnWithSize> columns_with_sizes;

    auto column_sizes = storage->getColumnSizes();
    auto column_names_and_types = storage_snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns());

    if (!column_names_allowed_to_select.empty())
    {
        auto it = column_names_and_types.begin();
        while (it != column_names_and_types.end())
        {
            if (!column_names_allowed_to_select.contains(it->name))
                it = column_names_and_types.erase(it);
            else
                ++it;
        }
    }

    if (!column_sizes.empty())
    {
        for (auto & column_name_and_type : column_names_and_types)
        {
            auto it = column_sizes.find(column_name_and_type.name);
            if (it == column_sizes.end())
                continue;

            columns_with_sizes.emplace_back(column_name_and_type, it->second);
        }
    }

    NameAndTypePair result;

    if (!columns_with_sizes.empty())
        result = std::min_element(columns_with_sizes.begin(), columns_with_sizes.end())->column;
    else
        /// If we have no information about columns sizes, choose a column of minimum size of its data type
        result = ExpressionActions::getSmallestColumn(column_names_and_types);

    return result;
}

void prepareBuildQueryPlanForTableExpression(const QueryTreeNodePtr & table_expression, PlannerContextPtr & planner_context)
{
    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    auto & table_expression_data = planner_context->getTableExpressionDataOrThrow(table_expression);
    auto columns_names = table_expression_data.getColumnNames();

    auto * table_node = table_expression->as<TableNode>();
    auto * table_function_node = table_expression->as<TableFunctionNode>();
    auto * query_node = table_expression->as<QueryNode>();
    auto * union_node = table_expression->as<UnionNode>();

    /** The current user must have the SELECT privilege.
      * We do not check access rights for table functions because they have been already checked in ITableFunction::execute().
      */
    NameSet columns_names_allowed_to_select;
    if (table_node)
    {
        const auto & column_names_with_aliases = table_expression_data.getSelectedColumnsNames();
        columns_names_allowed_to_select = checkAccessRights(*table_node, column_names_with_aliases, query_context);
    }

    if (columns_names.empty())
    {
        NameAndTypePair additional_column_to_read;

        if (table_node || table_function_node)
        {
            const auto & storage = table_node ? table_node->getStorage() : table_function_node->getStorage();
            const auto & storage_snapshot = table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot();
            additional_column_to_read = chooseSmallestColumnToReadFromStorage(storage, storage_snapshot, columns_names_allowed_to_select);
        }
        else if (query_node || union_node)
        {
            const auto & projection_columns = query_node ? query_node->getProjectionColumns() : union_node->computeProjectionColumns();
            NamesAndTypesList projection_columns_list(projection_columns.begin(), projection_columns.end());
            additional_column_to_read = ExpressionActions::getSmallestColumn(projection_columns_list);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected table, table function, query or union. Actual {}",
                            table_expression->formatASTForErrorMessage());
        }

        auto & global_planner_context = planner_context->getGlobalPlannerContext();
        const auto & column_identifier = global_planner_context->createColumnIdentifier(additional_column_to_read, table_expression);
        columns_names.push_back(additional_column_to_read.name);
        table_expression_data.addColumn(additional_column_to_read, column_identifier);
    }

    /// Limitation on the number of columns to read
    if (settings.max_columns_to_read && columns_names.size() > settings.max_columns_to_read)
        throw Exception(ErrorCodes::TOO_MANY_COLUMNS,
            "Limit for number of columns to read exceeded. Requested: {}, maximum: {}",
            columns_names.size(),
            settings.max_columns_to_read);
}


JoinTreeQueryPlan buildJoinTreeQueryPlan(const QueryTreeNodePtr & query_node,
    const SelectQueryInfo & select_query_info,
    SelectQueryOptions & select_query_options,
    const ColumnIdentifierSet & outer_scope_columns,
    PlannerContextPtr & planner_context)
{
    auto table_expressions_stack = buildTableExpressionsStack(query_node->as<QueryNode &>().getJoinTree());
    size_t table_expressions_stack_size = table_expressions_stack.size();
    bool is_single_table_expression = table_expressions_stack_size == 1;

    std::vector<ColumnIdentifierSet> table_expressions_outer_scope_columns(table_expressions_stack_size);
    ColumnIdentifierSet current_outer_scope_columns = outer_scope_columns;

    if (is_single_table_expression)
    {
        auto * table_node = table_expressions_stack[0]->as<TableNode>();
        if (table_node && shouldIgnoreQuotaAndLimits(*table_node))
        {
            select_query_options.ignore_quota = true;
            select_query_options.ignore_limits = true;
        }
    }
    else
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only support single table now.");
    }

    /// For each table, table function, query, union table expressions prepare before query plan build
    for (size_t i = 0; i < table_expressions_stack_size; ++i)
    {
        const auto & table_expression = table_expressions_stack[i];
        auto table_expression_type = table_expression->getNodeType();
        if (table_expression_type == QueryTreeNodeType::JOIN ||
            table_expression_type == QueryTreeNodeType::ARRAY_JOIN)
            continue;

        prepareBuildQueryPlanForTableExpression(table_expression, planner_context);
    }

    /** If left most table expression query plan is planned to stage that is not equal to fetch columns,
      * then left most table expression is responsible for providing valid JOIN TREE part of final query plan.
      *
      * Examples: Distributed, LiveView, Merge storages.
      */
    auto left_table_expression = table_expressions_stack.front();
    auto left_table_expression_query_plan = buildQueryPlanForTableExpression(left_table_expression,
        select_query_info,
        select_query_options,
        planner_context,
        is_single_table_expression);
    if (left_table_expression_query_plan.from_stage != QueryProcessingStage::FetchColumns)
        return left_table_expression_query_plan;

    return left_table_expression_query_plan;
}

void OptimizerPlanner::buildPlanForQueryNode()
{
    ProfileEvents::increment(ProfileEvents::SelectQueriesWithSubqueries);
    ProfileEvents::increment(ProfileEvents::QueriesWithSubqueries);

    auto & query_node = query_tree->as<QueryNode &>();
    const auto & query_context = planner_context->getPlannerContextPtr()->getQueryContext();

    if (query_node.hasWhere())
    {
        auto condition_constant = tryExtractConstantFromConditionNode(query_node.getWhere());
        if (condition_constant.has_value() && *condition_constant)
            query_node.getWhere() = {};
    }

    SelectQueryInfo select_query_info = buildSelectQueryInfo();

    StorageLimitsList current_storage_limits = storage_limits;
    select_query_info.local_storage_limits = buildStorageLimits(*query_context, select_query_options);
    current_storage_limits.push_back(select_query_info.local_storage_limits);
    select_query_info.storage_limits = std::make_shared<StorageLimitsList>(current_storage_limits);
    select_query_info.has_order_by = query_node.hasOrderBy();
    select_query_info.has_window = hasWindowFunctionNodes(query_tree);
    select_query_info.has_aggregates = hasAggregateFunctionNodes(query_tree);
    select_query_info.need_aggregate = query_node.hasGroupBy() || select_query_info.has_aggregates;

    if (!select_query_info.need_aggregate && query_node.hasHaving())
    {
        if (query_node.hasWhere())
            query_node.getWhere() = mergeConditionNodes({query_node.getWhere(), query_node.getHaving()}, query_context);
        else
            query_node.getWhere() = query_node.getHaving();

        query_node.getHaving() = {};
    }

    collectSets(query_tree, *planner_context->getPlannerContextPtr());

    collectTableExpressionData(query_tree, planner_context->getPlannerContextPtr());

    const auto & table_filters = planner_context->getPlannerContextPtr()->getGlobalPlannerContext()->filters_for_table_expressions;
    if (!select_query_options.only_analyze && !table_filters.empty()) // && top_level)
    {
        for (auto & [table_node, table_expression_data] : planner_context->getPlannerContextPtr()->getTableExpressionNodeToData())
        {
            auto it = table_filters.find(table_node);
            if (it != table_filters.end())
            {
                const auto & filters = it->second;
                table_expression_data.setFilterActions(filters.filter_actions);
                table_expression_data.setPrewhereInfo(filters.prewhere_info);
            }
        }
    }

    JoinTreeQueryPlan join_tree_query_plan;

    auto top_level_identifiers = collectTopLevelColumnIdentifiers(query_tree, planner_context->getPlannerContextPtr());

    join_tree_query_plan = buildJoinTreeQueryPlan(query_tree,
        select_query_info,
        select_query_options,
        top_level_identifiers,
        planner_context->getPlannerContextPtr());

    auto from_stage = join_tree_query_plan.from_stage;
    query_plan = std::move(join_tree_query_plan.query_plan);
    used_row_policies = std::move(join_tree_query_plan.used_row_policies);
    auto & mapping = join_tree_query_plan.query_node_to_plan_step_mapping;
    query_node_to_plan_step_mapping.insert(mapping.begin(), mapping.end());

    LOG_TRACE(getLogger("Planner"), "Query {} from stage {} to stage {}{}",
    query_tree->formatConvertedASTForErrorMessage(),
    QueryProcessingStage::toString(from_stage),
    QueryProcessingStage::toString(select_query_options.to_stage),
    select_query_options.only_analyze ? " only analyze" : "");

    if (select_query_options.to_stage == QueryProcessingStage::FetchColumns)
        return;

    query_node_to_plan_step_mapping[&query_node] = query_plan.getRootNode();

}


}
