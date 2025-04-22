#include <Query/Statistics/ASTHelpers.h>

#include <Access/ContextAccess.h>
#include <Query/Statistics/CatalogAdaptor.h>
#include <Interpreters/StorageID.h>
#include <Storages/StorageMaterializedView.h>

namespace DB::QueryStatistics
{
std::vector<StatsTableIdentifier> getTablesFromScope(ContextPtr context, const StatisticsScope & scope)
{
    std::vector<StatsTableIdentifier> tables;
    auto catalog = createCatalogAdaptor(context);

    if (!scope.database)
    {
        const auto access = context->getAccess();
        const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_DATABASES);
        for (const auto & [database_name, db] : DatabaseCatalog::instance().getDatabases())
        {
            if (check_access_for_databases && !access->isGranted(AccessType::SHOW_DATABASES, database_name))
                continue;

            if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
                continue; /// We don't want to show the internal database for temporary tables in system.databases

            auto new_tables = catalog->getAllTablesID(database_name);
            tables.insert(tables.end(), new_tables.begin(), new_tables.end());
        }
    }
    else
    {
        auto db = context->resolveDatabase(scope.database.value());
        if (!scope.table)
        {
            tables = catalog->getAllTablesID(db);
        }
        else
        {
            auto table = scope.table.value();
            auto table_info_opt = catalog->getTableIdByName(db, table);
            if (!table_info_opt)
            {
                throw Exception(ErrorCodes::UNKNOWN_TABLE, "Unknown Table ({}) in database ({})", table, db);
            }
            tables.emplace_back(table_info_opt.value());
        }
    }

    // ensure table is unique
    std::unordered_set<StatsTableIdentifier> table_set;
    std::vector<StatsTableIdentifier> result;
    // show materialized view as target table
    for (auto table : tables)
    {
        auto storage = catalog->getStorageByTableId(table);
        if (const auto * mv = dynamic_cast<const StorageMaterializedView *>(storage.get()))
        {
            auto table_opt = catalog->getTableIdByName(mv->getTargetTableId().getDatabaseName(), mv->getTargetTableId().getTableName());
            if (!table_opt.has_value())
            {
                LOG_WARNING(
                    getLogger("ShowStats"),
                    "mv {}.{} has invalid target table {}.{}",
                    mv->getStorageID().getDatabaseName(),
                    mv->getStorageID().getTableName(),
                    mv->getTargetTableId().getDatabaseName(),
                    mv->getTargetTableId().getTableName());
                continue;
            }
            table = table_opt.value();
        }
        if (table_set.count(table))
        {
            continue;
        }
        table_set.insert(table);
        result.emplace_back(table);
    }
    return result;
}
}
