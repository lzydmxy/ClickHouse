#include <Query/Processors/QueryPlan/TableScanStepExt.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/StorageReplicatedMergeTree.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_PREWHERE;
    extern const int INVALID_LIMIT_EXPRESSION;
    extern const int PROJECTION_SELECTION_ERROR;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int INCOMPATIBLE_COLUMNS;
}

namespace _scan_execute_impl
{
    // a struct used in projection selection with any necessary intermediate states
    struct ProjectionMatchContext;
    using ProjectionMatchContexts = std::vector<ProjectionMatchContext>;

    //const UInt32 NODE_ID_TABLE_SCAN = 0;
    //const UInt32 NODE_ID_FILTER = 1;
    //const UInt32 NODE_ID_PROJECTION = 2;
    //const UInt32 NODE_ID_AGGREGATION = 3;

    /// implementations
    struct ProjectionMatchContext
    {
        const ProjectionDescription & projection_desc;
        NamesAndTypesList missing_columns;
        //SymbolTranslationMap column_translation;
        NameToType column_types;
        Assignments rewritten_assignments;
        NameToType rewritten_types;
        ASTPtr rewritten_filter;
        NameSet required_column_set;
        //MergeTreeDataSelectAnalysisResultPtr read_analysis;

        ProjectionMatchContext(const ProjectionDescription & projection_desc_,
            //const PartGroup & part_group,
            const IStorage * storage,
            const NamesAndTypesList & table_columns);

        //ExecutePlanElement buildExecutePlanElement(PartGroup part_group, const SelectQueryInfo & query_info) const;

        Names requiredColumns() const
        {
            Names result {required_column_set.begin(), required_column_set.end()};
            return result;
        }
    };

    ProjectionMatchContext::ProjectionMatchContext(
        const ProjectionDescription & projection_desc_,
        //const PartGroup & part_group,
        const IStorage * storage,
        const NamesAndTypesList & table_columns)
        : projection_desc(projection_desc_)
    {
        //const auto & data_part_columns = part_group.getColumns();
        NamesAndTypesList data_part_columns;

        for (const auto & column: table_columns)
            if (!data_part_columns.contains(column.name))
                missing_columns.push_back(column);

        for (size_t i = 0; i < projection_desc.required_columns.size(); ++i)
        {
            //column_translation.addStorageTranslation(projection_desc.column_asts[i], projection_desc.column_names[i], storage, NODE_ID_TABLE_SCAN);
            //column_types.emplace(projection_desc.required_columns[i], projection_desc.data_types[i]);
        }
    }

    /*
    ExecutePlanElement ProjectionMatchContext::buildExecutePlanElement(PartGroup part_group, const SelectQueryInfo & query_info) const
    {
        std::shared_ptr<FilterStep> rewritten_filter_step = nullptr;
        std::shared_ptr<ProjectionStep> rewritten_projection_step = nullptr;
        auto require_columns = requiredColumns();

        NamesAndTypes names_and_types;
        for (const auto & column: require_columns)
            names_and_types.emplace_back(column, column_types.at(column));

        DataStream input_stream {.header = Block{names_and_types}};

        if (rewritten_filter)
            rewritten_filter_step = std::make_shared<FilterStep>(input_stream, rewritten_filter);

        rewritten_projection_step = std::make_shared<ProjectionStep>(rewritten_filter ? rewritten_filter_step->getOutputStream(): input_stream, rewritten_assignments, rewritten_types);

        ActionsDAGPtr prewhere_actions;

        if (query_info.prewhere_info)
        {
            auto & prewhere_info = query_info.prewhere_info;
            prewhere_actions = prewhere_info->prewhere_actions->clone();
            prewhere_actions->foldActionsByProjection(
                required_column_set, projection_desc.sample_block_for_keys, prewhere_info->prewhere_column_name, false);

            auto & outputs = prewhere_actions->getOutputs();
            for (const auto & input: prewhere_actions->getInputs())
                if (std::find(outputs.begin(), outputs.end(), input) == outputs.end())
                    outputs.emplace_back(input);
        }

        return ExecutePlanElement {
            std::move(part_group),
            read_analysis,
            &projection_desc,
            std::move(require_columns),
            rewritten_projection_step,
            rewritten_filter_step,
            prewhere_actions
        };
    }
    */

    struct PartSchemaKey
    {
        // use ordered set to ensure hash function stable
        std::set<std::string_view> columns;
        std::set<std::string_view> projections;

        explicit PartSchemaKey(MergeTreeData::DataPartPtr part);

        bool operator==(const PartSchemaKey & other) const
        {
            return columns == other.columns && projections == other.projections;
        }

        struct Hash
        {
            size_t operator()(const PartSchemaKey & key) const
            {
                size_t res = 17;

                for (const auto & column: key.columns)
                    res = res * 31 + std::hash<std::string_view>()(column);

                for (const auto & projection: key.projections)
                    res = res * 31 + std::hash<std::string_view>()(projection);

                return res;
            }
        };
    };


    PartSchemaKey::PartSchemaKey(MergeTreeData::DataPartPtr part)
    {
        std::vector<std::string_view> columns_vector;
        std::vector<std::string_view> projections_vector;

        for (const auto & column_entry: part->getColumns())
            columns_vector.emplace_back(column_entry.name);
        for (const auto & projection_entry: part->getProjectionParts())
            projections_vector.emplace_back(projection_entry.first);

        columns.insert(columns_vector.begin(), columns_vector.end());
        projections.insert(projections_vector.begin(), projections_vector.end());
    }
}

using namespace _scan_execute_impl;

class TableScanExecutor
{
public:
    TableScanExecutor(TableScanStepExt & step, const MergeTreeData & storage_, ContextPtr context_);
    //ExecutePlan buildExecutePlan(const DistributedPipelineSettings & distributed_settings);

private:
    bool match(ProjectionMatchContext & candidate) const;
    //MergeTreeDataSelectAnalysisResultPtr estimateReadMarks(const PartGroup & part_group) const;
    //void estimateReadMarksForProjection(const PartGroup & part_group, ProjectionMatchContext & candidate) const;
    void prunePartsByIndex(MergeTreeData::DataPartsVector & parts) const;
    //PartGroups groupPartsBySchema(const MergeTreeData::DataPartsVector & parts);
    ASTPtr rewriteExpr(ASTPtr expr, ProjectionMatchContext & candidate) const;

    struct NameWithAST
    {
        String name;
        ASTPtr flatten_ast;
    };

    bool match_projection = false;

    const MergeTreeData & storage;
    StorageMetadataPtr storage_metadata;
    MergeTreeDataSelectExecutor merge_tree_reader;
    const SelectQueryInfo & select_query_info;
    ContextPtr context;
    LoggerPtr log;

    bool has_aggregate;
    //todo: need to implement query_lineage
    //std::optional<SymbolTransformMap> query_lineage;
    Names query_required_columns;
    NameToType column_types_before_agg;
    std::vector<NameWithAST> aggregate_keys;
    std::vector<NameWithAST> aggregate_descs;
    ASTPtr flatten_filter;
    ASTPtr flatten_prewhere;
    std::shared_ptr<PartitionIdToMaxBlock> max_added_blocks;
};

TableScanExecutor::TableScanExecutor(TableScanStepExt & step, const MergeTreeData & storage_, ContextPtr context_)
    : storage(storage_)
    , storage_metadata(storage.getInMemoryMetadataPtr())
    , merge_tree_reader(storage)
    , select_query_info(step.getQueryInfo())
    , context(std::move(context_))
    , log(getLogger("TableScanExecutor"))
{
    if (storage_metadata->projections.empty())
        return;

    if (step.hasInlineExpressions())
        return;

    //has_aggregate = step.getPushdownAggregation() != nullptr;
    query_required_columns = step.getRequiredColumns(TableScanStepExt::OutputAndPrewhere);
    /*
    query_lineage = [&]() {
        PlanNodePtr node;
        QueryPlanStepPtr table_scan_without_pushdown_steps = std::make_shared<TableScanStepExt>(
            context,
            step.getStorageID(),
            step.getColumnAlias(),
            step.getQueryInfo(),
            step.getMaxBlockSize());
        node = PlanNodeBase::createPlanNode(NODE_ID_TABLE_SCAN, table_scan_without_pushdown_steps);

        if (const auto & filter = step.getPushdownFilter())
            node = PlanNodeBase::createPlanNode(NODE_ID_FILTER, filter, {node});

        if (const auto &  projection = step.getPushdownProjection())
            node = PlanNodeBase::createPlanNode(NODE_ID_PROJECTION, projection, {node});

        if (const auto &  aggregation = step.getPushdownAggregation())
            node = PlanNodeBase::createPlanNode(NODE_ID_AGGREGATION, aggregation, {node});

        return SymbolTransformMap::buildFrom(*node);
    }();

    if (!query_lineage)
        return;

    if (has_aggregate)
    {
        const auto * query_aggregate = step.getPushdownAggregationCast();
        column_types_before_agg = query_aggregate->getInputStreams()[0].header.getNamesToTypes();

        for (const auto & origin_grouping_key: query_aggregate->getKeys())
            aggregate_keys.emplace_back(NameWithAST{origin_grouping_key, query_lineage->inlineReferences(origin_grouping_key)});

        for (const auto & query_aggregate_desc: query_aggregate->getAggregates())
            aggregate_descs.emplace_back(NameWithAST{query_aggregate_desc.column_name,
                                                     query_lineage->inlineReferences(query_aggregate_desc.column_name)});
    }
    */

    if (const auto * query_filter_step = step.getPushdownFilterCast())
    {
        //const auto & query_filter = query_filter_step->getFilter();
        //flatten_filter = query_lineage->inlineReferences(query_filter);
    }

    ASTSelectQueryPtr select_query;
    //auto select_query = select_query_info.query;

    if (auto prewhere = select_query->prewhere())
    {
        NameSet columns{step.getColumnNames().begin(), step.getColumnNames().end()};
        //flatten_prewhere = IdentifierToColumnReference::rewrite(step.getStorage().get(), NODE_ID_TABLE_SCAN, prewhere);
    }

    const auto & settings = context->getSettingsRef();
    if (settings.select_sequential_consistency)
    {
        if (auto replicated = dynamic_pointer_cast<const StorageReplicatedMergeTree>(storage.shared_from_this()))
            max_added_blocks = std::make_shared<PartitionIdToMaxBlock>(replicated->getMaxAddedBlocks());
    }

    match_projection = true;
}

/*
ExecutePlan TableScanExecutor::buildExecutePlan(const DistributedPipelineSettings & distributed_settings)
{
    if (!match_projection)
        return {};

    PartGroups part_groups;
    {
        auto parts = storage.getDataPartsVector();
        if (distributed_settings.source_task_filter.isValid())
        {
            auto size_before_filtering = parts.size();
            filterParts(parts, distributed_settings.source_task_filter);
            LOG_TRACE(
                log,
                "After filtering({}) the number of parts of table {} becomes {} from {}",
                distributed_settings.source_task_filter.toString(),
                storage.getTableName(),
                parts.size(),
                size_before_filtering);
        }
        parts.erase(std::remove_if(parts.begin(), parts.end(), [](auto & part) { return part->info.isFakeDropRangePart(); }), parts.end());

        LOG_DEBUG(log, "Num of parts before part pruning: {}", std::to_string(parts.size()));

        prunePartsByIndex(parts);

        LOG_DEBUG(log, "Num of parts after part pruning: {}", std::to_string(parts.size()));

        if (parts.size() > 100000)
            throw Exception(ErrorCodes::PROJECTION_SELECTION_ERROR, "Projection selection error: too many parts before part grouping");

        part_groups = groupPartsBySchema(parts);

        LOG_DEBUG(log, "Num of part groups before part grouping: {}", std::to_string(part_groups.size()));

        if (part_groups.size() > 100)
            throw Exception(ErrorCodes::PROJECTION_SELECTION_ERROR, "Projection selection error: Too many part groups before projection selection");
    }

    ExecutePlan execute_plan;
    auto table_columns = storage_metadata->getColumns().getAllPhysical();

    for (auto & part_group: part_groups)
    {
        ProjectionMatchContexts projection_candidates;

        for (const auto & projection_desc: storage_metadata->projections)
            if (part_group.hasProjection(projection_desc.name))
            {
                projection_candidates.emplace_back(projection_desc, part_group, &storage, table_columns);
                if (!match(projection_candidates.back()))
                    projection_candidates.pop_back();
            }

        ProjectionMatchContext * selected_candidate = nullptr;
        size_t min_sum_marks = std::numeric_limits<size_t>::max();

        auto normal_read_result = estimateReadMarks(part_group);

        if (!normal_read_result->error())
            min_sum_marks = normal_read_result->marks() + 1;

        for (auto & candidate: projection_candidates)
        {
            estimateReadMarksForProjection(part_group, candidate);
            if (!candidate.read_analysis->error() && candidate.read_analysis->marks() < min_sum_marks)
            {
                selected_candidate = &candidate;
                min_sum_marks = candidate.read_analysis->marks();
            }
        }

        if (selected_candidate)
            execute_plan.push_back(selected_candidate->buildExecutePlanElement(std::move(part_group), select_query_info));
        else
            execute_plan.push_back(ExecutePlanElement(part_group, std::move(normal_read_result), part_group.parts));
    }

    return execute_plan;
}
*/

bool TableScanExecutor::match(ProjectionMatchContext & candidate) const
{
    const auto & projection_desc = candidate.projection_desc;

    if (!has_aggregate && projection_desc.type == ProjectionDescription::Type::Aggregate)
        return false;

    // quick filter by required columns, proceed to match if query required columns belong to
    // projection required columns.
    //
    // e.g., given
    //   query: select toYear(date), sum(amount) from t1 group by toYear(date);
    //   projection: select toYear(date), key, sum(amount) from t1 group by toYear(date), key;
    // it produce:
    //   query required columns: date, amount
    //   projection require columns: date, key, amount
    NameSet projection_required_columns {projection_desc.required_columns.begin(), projection_desc.required_columns.end()};
    for (const auto & query_column: query_required_columns)
        if (!projection_required_columns.contains(query_column) && !candidate.missing_columns.contains(query_column))
            return false;

    // translate query column based expression to projection column based expression,
    // this is implemented by below steps:
    //   1. flatten query column based expression to table column based expression,
    //      by query column lineage information
    //   2. translate table column based expression to projection column,
    //      by projection column translation information
    //   3. if there are table columns unable to translate, either it's a missing key,
    //      rewrite to a literal with the default value of the column type; or this
    //      projection can not serve this query
    //
    // e.g. given
    //   query plan: Aggregating(key: $3, function: sum($2)) -> Projection($3 := toYear($1) + 1) -> TableScan($1 := date, $2 := amount)
    //   projection: select toYear(date), key, sum(amount) from t1 group by toYear(date), key;
    // the procedure of rewriting grouping key(i.e. `$3`) is as below:
    //   1. flatten `$3` => toYear(`$1`) + 1 => toYear(`date`) + 1
    //   2. translate toYear(`date`) + 1 => `toYear(date)` + 1
    if (projection_desc.type == ProjectionDescription::Type::Aggregate)
    {
        // match & rewrite grouping keys
        for (const auto & aggregate_key: aggregate_keys)
        {
            auto rewritten_expr = rewriteExpr(aggregate_key.flatten_ast, candidate);
            if (!rewritten_expr)
                return false;
            candidate.rewritten_assignments.emplace_back(aggregate_key.name, rewritten_expr);
            candidate.rewritten_types.emplace(aggregate_key.name, column_types_before_agg.at(aggregate_key.name));
        }

        // match & rewrite aggregates
        /*
        for (const auto & aggregate_desc: aggregate_descs)
        {
            auto projection_agg_column_opt = candidate.column_translation.tryGetTranslation(aggregate_desc.flatten_ast);
            if (!projection_agg_column_opt)
                return false;
            auto & projection_agg_column = *projection_agg_column_opt;
            candidate.rewritten_assignments.emplace_back(aggregate_desc.name, std::make_shared<ASTIdentifier>(projection_agg_column));
            candidate.rewritten_types.emplace(aggregate_desc.name, candidate.column_types.at(projection_agg_column));
            candidate.required_column_set.emplace(projection_agg_column);
        }
        */
        // match & rewrite where
        if (flatten_filter)
        {
            auto rewritten_filter = rewriteExpr(flatten_filter, candidate);
            if (!rewritten_filter)
                return false;
            candidate.rewritten_filter = std::move(rewritten_filter);
        }

        // partition filter always matches, since projection parts have the same partition schema as the raw parts

        // match prewhere
        // the real rewrite action(`foldActionsByProjection`) is deferred to `buildExecutePlanElement`
        if (flatten_prewhere)
        {
            auto rewritten_prewhere = rewriteExpr(flatten_prewhere, candidate);
            if (!rewritten_prewhere)
                return false;
        }

        return true;
    }
    else
    {
        // to prevent a query both use normal projection & aggregate projection
        if (has_aggregate)
            return false;

        return false;
    }
}

/*
MergeTreeDataSelectAnalysisResultPtr TableScanExecutor::estimateReadMarks(const PartGroup & part_group) const
{
    return merge_tree_reader.estimateNumMarksToRead(part_group.parts,
        query_required_columns,
        storage_metadata,
        storage_metadata,
        select_query_info,
        context,
        context->getSettingsRef().max_threads,
        max_added_blocks);
}

void TableScanExecutor::estimateReadMarksForProjection(const PartGroup & part_group, ProjectionMatchContext & candidate) const
{
    MergeTreeData::DataPartsVector projection_parts;

    for (const auto & part: part_group.parts)
    {
        const auto & projections = part->getProjectionParts();
        auto it = projections.find(candidate.projection_desc.name);
        if (it == projections.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "projection not found in a part group, does part grouping perform correctly?");
        projection_parts.push_back(it->second);
    }

    candidate.read_analysis = merge_tree_reader.estimateNumMarksToRead(projection_parts,
        candidate.requiredColumns(),
        storage_metadata,
        candidate.projection_desc.metadata,
        select_query_info,
        context,
        context->getSettingsRef().max_threads,
        max_added_blocks);

}

void TableScanExecutor::prunePartsByIndex(MergeTreeData::DataPartsVector & parts) const
{
    if (parts.empty())
        return;

    auto part_values = MergeTreeDataSelectExecutor::filterPartsByVirtualColumns(storage, parts, select_query_info, context);

    if (part_values && part_values->empty())
    {
        parts.clear();
        return;
    }

    ReadFromMergeTree::AnalysisResult result;
    MergeTreeDataSelectExecutor::filterPartsByPartition(
        parts, part_values, storage_metadata, storage, select_query_info, context, max_added_blocks.get(), log, result.index_stats);
}

PartGroups TableScanExecutor::groupPartsBySchema(const MergeTreeData::DataPartsVector & parts)
{
    std::unordered_map<PartSchemaKey, MergeTreeData::DataPartsVector, PartSchemaKey::Hash> parts_by_schema;

    for (const auto & part: parts)
    {
        PartSchemaKey key(part);
        parts_by_schema[std::move(key)].emplace_back(part);
    }

    PartGroups groups;

    for (auto & it : parts_by_schema)
        groups.emplace_back(PartGroup{std::move(it.second)});

    return groups;
}
*/

ASTPtr TableScanExecutor::rewriteExpr(ASTPtr expr, ProjectionMatchContext & candidate) const
{
    /*
    if (auto projection_column = candidate.column_translation.tryGetTranslation(expr))
    {
        candidate.required_column_set.emplace(*projection_column);
        return std::make_shared<ASTIdentifier>(*projection_column);
    }

    if (auto * col_ref = expr->as<ASTTableColumnReference>())
    {
        if (auto pair = candidate.missing_columns.tryGetByName(col_ref->column_name))
            return std::make_shared<ASTLiteral>(pair->type->getDefault());

        return nullptr;
    }
    else if (auto * func = expr->as<ASTFunction>(); func && func->arguments && !func->arguments->children.empty())
    {
        ASTs rewritten_arguments;

        for (const auto & argument: func->arguments->children)
        {
            rewritten_arguments.push_back(rewriteExpr(argument, candidate));
            if (rewritten_arguments.back() == nullptr)
                return nullptr;
        }

        return makeASTFunction(func->name, rewritten_arguments);
    }
    else
        return expr;
    */
   return nullptr;
}

}
