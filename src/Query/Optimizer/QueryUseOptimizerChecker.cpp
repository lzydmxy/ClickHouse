#include <Query/Optimizer/QueryUseOptimizerChecker.h>

#include <Query/Analyzer/QueryAnalyzer.h>
#include <Query/Analyzer/QueryRewriter.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Storages/StorageView.h>
#include <Interpreters/executeQuery.h>
#include <Query/Parsers/ASTExplainQueryExt.h>
#include <Parsers/ASTInsertQuery.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int UNSUPPORTED_PARAMETER;
}

void changeASTSettings(ASTPtr &node)
{
    if (!node)
        return;

    if (auto * select = node->as<ASTSelectQuery>())
    {
        const auto & settings_ptr = select->settings();
        if (!settings_ptr)
            return;
        auto & ast = settings_ptr->as<ASTSetQuery &>();
        for (auto & change : ast.changes)
        {
            if (change.name == "enable_distributed_stages")
                change.value = Field(false);
            else if (change.name == "enable_optimizer")
                change.value = Field(false);
        }
    }
    for (auto & child : node->children)
        changeASTSettings(child);
}

void turnOffOptimizer(ContextMutablePtr context, ASTPtr & node)
{
    SettingsChanges setting_changes;

    setting_changes.emplace_back("enable_optimizer", false);

    context->applySettingsChanges(setting_changes);
    changeASTSettings(node);
}

static bool checkDatabaseAndTable(String database_name, String table_name, ContextMutablePtr context, const NameSet & ctes, String & reason)
{
    /// not with table
    if (database_name.empty() && ctes.contains(table_name))
        return true;

    /// If the database is not specified - use the current database.
    auto table_id = context->tryResolveStorageID(StorageID(database_name, table_name));
    auto storage_table = DatabaseCatalog::instance().tryGetTable(table_id, context);
    if (database_name.empty() && !storage_table)
        database_name = context->getCurrentDatabase();

    if (!storage_table)
    {
        reason = fmt::format("table not found: {}.{}", database_name, table_name);
        return false;
    }

    if (database_name == "system")
        return true;

    if (dynamic_cast<const StorageView *>(storage_table.get()))
    {
        auto table_metadata_snapshot = storage_table->getInMemoryMetadataPtr();
        auto subquery = table_metadata_snapshot->getSelectQuery().inner_query;

        QueryUseOptimizerVisitor checker;
        QueryUseOptimizerContext check_context {.context = context};
        if (!ASTVisitorUtil::accept(subquery, checker, check_context))
        {
            reason = checker.getReason();
            return false;
        }
        return true;
    }

    // todo: hongzhigao1, need storage
    // if (!storage_table->supportsOptimizer())
    // {
    //     reason = fmt::format("unsupport storage {}: {}.{}", storage_table->getName(), database_name, table_name);
    //     return false;
    // }
    return true;
}

bool QueryUseOptimizerChecker::check(ASTPtr node, ContextMutablePtr context, bool throw_exception)
{
    if (!context->getOptimizerContext()->getSettingsRef().enable_optimizer && context->getOptimizerContext()->getSettingsRef().enable_distributed_output)
        throw Exception(ErrorCodes::UNSUPPORTED_PARAMETER, "Distributed output in non-optimizer mode is not supported, please enable optimizer.");

    if (!node || !context->getOptimizerContext()->getSettingsRef().enable_optimizer)
    {
        turnOffOptimizer(context, node);
        return false;
    }

    // Optimizer only work for Server.
    // for example INSERT INTO parallel_replicas_backup(d, x, u, s) SELECT d, x, u, s FROM parallel_replicas;
    // will execute query : INSERT INTO test.parallel_replicas_backup_4313395779120660490 (d, x, u, s) SELECT d, x, u, s FROM test.parallel_replicas )
    // will execute query : SELECT d, x, u, s FROM test.parallel_replicas_4313395779120660490
    // in worker.
    // todo: hongzhigao1, other feat: chch
    // if (context->getServerType() == ServerType::cnch_worker)
    // {
    //     turnOffOptimizer(context, node);
    //     return false;
    // }

    String reason;
    if (auto * explain = node->as<ASTExplainQueryExt>())
    {
        bool explain_plan = explain->getKind() == ASTExplainQueryExt::ExplainKindExt::OptimizerPlan
            || explain->getKind() == ASTExplainQueryExt::ExplainKindExt::QueryPlan
            || explain->getKind() == ASTExplainQueryExt::ExplainKindExt::QueryPipeline
            || explain->getKind() ==  ASTExplainQueryExt::AnalyzedSyntax
            || explain->getKind() ==  ASTExplainQueryExt::DistributedAnalyze
            || explain->getKind() ==  ASTExplainQueryExt::LogicalAnalyze
            || explain->getKind() ==  ASTExplainQueryExt::PipelineAnalyze
            || explain->getKind() ==  ASTExplainQueryExt::Distributed
            || explain->getKind() ==  ASTExplainQueryExt::TraceOptimizerRule
            || explain->getKind() ==  ASTExplainQueryExt::TraceOptimizer
            || explain->getKind() ==  ASTExplainQueryExt::MetaData;
        if (!explain_plan)
            reason = "unsupported explain type";
        return explain_plan && check(explain->getExplainedQuery(), context, throw_exception);
    }
    // todo: hongzhigao1, other feat: ASTCreatePreparedStatementQuery
    // if (auto * prepare = node->as<ASTCreatePreparedStatementQuery>())
    // {
    //     return check(prepare->getQuery(), context, throw_exception);
    // }


    bool support = false;

    if (node->as<ASTSelectQuery>() || node->as<ASTSelectWithUnionQuery>() || node->as<ASTSelectIntersectExceptQuery>())
    {
        // disable system query, table function, no merge tree table
        NameSet with_tables;

        QueryUseOptimizerVisitor checker;
        QueryUseOptimizerContext check_context{.context = context};
        try
        {
            support = ASTVisitorUtil::accept(node, checker, check_context);
        }
        catch (Exception &)
        {
            //            if (e.code() != ErrorCodes::NOT_IMPLEMENTED)
            throw;
            //            support = false;
        }

        if (!support)
        {
            LOG_INFO(getLogger("QueryUseOptimizerChecker"), "query is unsupported for optimizer, reason: {}", checker.getReason());
            reason = checker.getReason();
        }
    }
    else if (node->as<ASTInsertQuery>())
    {
        support = true;
        auto * insert_query = node->as<ASTInsertQuery>();
        if (insert_query->infile || insert_query->table_function || !insert_query->select)
        {
            reason = "unsupported function/in file/no select";
            support = false;
        }
        else
        {
            auto database = insert_query->table_id.database_name;
            if (database.empty())
                database = context->getCurrentDatabase();

            if (!checkDatabaseAndTable(database, insert_query->table_id.getTableName(), context, {}, reason))
                support = false;
        }

        LOG_DEBUG(getLogger("QueryUseOptimizerChecker"), "support: {}, check: {}", support, check(insert_query->select, context));
        if (support)
            support = check(insert_query->select, context, throw_exception);
    }

    if (!support)
    {
        if (throw_exception)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "query is unsupported for optimizer, reason: {}", reason);
        else
            turnOffOptimizer(context, node);
    }

    return support;
}

bool QueryUseOptimizerVisitor::visitNode(ASTPtr & node, QueryUseOptimizerContext & context)
{
    for (auto & child : node->children)
    {
        if (!ASTVisitorUtil::accept(child, *this, context))
        {
            return false;
        }
    }
    return true;
}

static bool
checkDatabaseAndTable(const ASTTableExpression & table_expression, const ContextMutablePtr & context, const NameSet & ctes, String & reason)
{
    if (table_expression.database_and_table_name)
    {
        auto db_and_table = DatabaseAndTableWithAlias(table_expression.database_and_table_name);
        return checkDatabaseAndTable(db_and_table.database, db_and_table.table, context, ctes, reason);
    }
    return true;
}

bool QueryUseOptimizerVisitor::visitASTSelectQueryExt(ASTPtr & node, QueryUseOptimizerContext & context)
{
    auto * select = node->as<ASTSelectQuery>();

    if (select->limit_with_ties)
    {
        reason = "LIMIT/OFFSET FETCH WITH TIES not implemented";
        return false;
    }

    if (context.disallow_subquery)
    {
        reason = "nullIn/globalNullIn/notNullIn/globalNotNullIn function with subquery not implemented";
        return false;
    }

    if (select->group_by_with_totals && context.disallow_with_totals)
    {
        reason = "group by with totals only supports with totals at outmost select";
        return false;
    }
    auto has_join = [](const auto & sel_query) { return sel_query.tables() && sel_query.tables()->children.size() > 1; };

    QueryUseOptimizerContext child_context{.context = context.context, .ctes = context.ctes, .disallow_with_totals = has_join(*select)};
    collectWithTableNames(*select, child_context.ctes);

    for (const auto * table_expression : getTableExpressions(*select))
    {
        if (!checkDatabaseAndTable(*table_expression, child_context.context, child_context.ctes, reason))
            return false;

        if (table_expression->table_function)
        {
            const auto & function = table_expression->table_function->as<ASTFunction &>();

            if (function.name == "fusionMerge")
                return true;

            reason = "table function";
            return false;
        }
    }

    return visitNode(node, child_context);
}

bool QueryUseOptimizerVisitor::visitASTTableJoin(ASTPtr & node, QueryUseOptimizerContext & context)
{
    return visitNode(node, context);
}

bool QueryUseOptimizerVisitor::visitASTIdentifier(ASTPtr & node, QueryUseOptimizerContext & context)
{
    bool support = !context.context->getExternalTables().contains(node->as<ASTIdentifier>()->name());
    if (!support)
        reason = "external table";
    return support;
}

bool QueryUseOptimizerVisitor::visitASTFunction(ASTPtr & node, QueryUseOptimizerContext & context)
{
    auto & fun = node->as<ASTFunction &>();
    if (fun.name == "untuple")
    {
        reason = "unsupported untuple function";
        return false;
    }

    else if (functionIsInOrGlobalInOperator(fun.name) && fun.arguments->children.size() == 2)
    {
        if (auto * identifier = fun.arguments->children[1]->as<ASTIdentifier>())
        {
            if (auto table = identifier->createTable())
            {
                ASTTableExpression table_expression;
                table_expression.database_and_table_name = table;
                if (!checkDatabaseAndTable(table_expression, context.context, context.ctes, reason))
                    return false;
            }
        }
    }
    bool disallow_subquery = context.disallow_subquery;
    context.disallow_subquery = disallow_subquery || (fun.name == "nullIn" || fun.name == "globalNullIn" || fun.name == "notNullIn" || fun.name == "globalNotNullIn");
    bool support = visitNode(node, context);
    context.disallow_subquery = disallow_subquery;
    return support;
}

bool QueryUseOptimizerVisitor::visitASTQuantifiedComparisonExt(ASTPtr & node, QueryUseOptimizerContext & context)
{
    return visitNode(node, context);
}

void QueryUseOptimizerVisitor::collectWithTableNames(ASTSelectQuery & query, NameSet & with_tables)
{
    if (auto with = query.with())
    {
        for (const auto & child : with->children)
            if (auto * with_elem = child->as<ASTWithElement>())
                with_tables.emplace(with_elem->name);
    }
}

}
