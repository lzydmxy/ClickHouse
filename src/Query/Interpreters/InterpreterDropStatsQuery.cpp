#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterFactory.h>
#include <Query/Interpreters/InterpreterDropStatsQuery.h>
#include <Query/Parsers/ASTStatsQueryExt.h>
#include <Query/Statistics/ASTHelpers.h>
#include <Query/Statistics/CatalogAdaptor.h>
#include <Query/Statistics/CatalogAdaptorProxy.h>
#include <Query/Statistics/DropHelper.h>
#include <Query/Statistics/StatsTableBasic.h>

namespace DB
{
using namespace QueryStatistics;
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_DATABASE;
}

BlockIO InterpreterDropStatsQuery::execute()
{
    auto context = getContext();
    auto query = query_ptr->as<const ASTDropStatsQueryExt>();
    auto catalog = QueryStatistics::createCatalogAdaptor(context);

    auto cache_policy = query->cache_policy;

    if (cache_policy == StatisticsCachePolicy::Default)
    {
        // use context settings
        cache_policy = context->getOptimizerContext()->getSettingsRef().statistics_cache_policy;
    }

    // when enable_memory_catalog is true, we won't use cache
    if (catalog->getSettingsRef().enable_memory_catalog)
    {
        if (cache_policy != StatisticsCachePolicy::Default)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "memory catalog don't support cache policy");
    }

    if (query->getTable() == "__reset")
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "unsupported");
    }

    catalog->checkHealth(/*is_write=*/true);

    auto proxy = QueryStatistics::createCatalogAdaptorProxy(catalog, cache_policy);
    auto db = context->resolveDatabase(query->getDatabase());

    if (!DatabaseCatalog::instance().isDatabaseExist(db))
    {
        throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Unknown database ({})", db);
    }

    auto tables = getTablesFromAST(context, query);


    if (tables.size() == 1 && !query->columns.empty())
    {
        const auto & table = tables[0];
        dropStatsColumns(context, table, query->columns, cache_policy, true);
    }
    else
    {
        for (const auto & table : tables)
        {
            dropStatsTable(context, table, cache_policy, true);
        }
    }

    return {};
}

void registerInterpreterDropStatsQuery(InterpreterFactory & factory)
{
    auto fn = [](const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDropStatsQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDropStatsQuery", fn);
}

}
