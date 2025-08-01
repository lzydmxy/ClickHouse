#include <Query/Statistics/CatalogAdaptor.h>

#include <Interpreters/DatabaseCatalog.h>
#include <Query/Statistics/SerdeUtils.h>
#include <Query/Statistics/StatisticsKeeperStore.h>
#include <Query/Statistics/TypeUtils.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <string>

namespace DB::QueryStatistics
{

class CatalogAdaptorKeeper : public CatalogAdaptor
{
public:
    CatalogAdaptorKeeper(ContextPtr context_, std::shared_ptr<StatisticsKeeperStore> statistics_keeper_store)
        : CatalogAdaptor(context_), statistics_keeper_store(statistics_keeper_store)
    {
    }

    bool hasStatsData(const StatsTableIdentifier & table) override
    {
        auto & statistics_keeper_store = getStatisticsKeeperStore();
        std::shared_lock lck(statistics_keeper_store.mtx);
        auto key = table.getUniqueKey();
        /// return whether table_stats of the corresponding table is non-empty
        return statistics_keeper_store.entries.count(key);
    }

    StatsData readStatsData(const StatsTableIdentifier & table) override
    {
        auto & statistics_keeper_store = getStatisticsKeeperStore();
        std::shared_lock lck(statistics_keeper_store.mtx);
        auto key = table.getUniqueKey();

        if (!statistics_keeper_store.entries.count(key))
        {
            return {};
        }

        return statistics_keeper_store.entries.at(key)->data;
    }


    std::vector<String> readStatsColumnsKey(const StatsTableIdentifier & table) override
    {
        std::vector<String> res;

        auto & statistics_keeper_store = getStatisticsKeeperStore();
        std::shared_lock lck(statistics_keeper_store.mtx);
        auto key = table.getUniqueKey();

        if (!statistics_keeper_store.entries.contains(key))
        {
            return {};
        }

        for (auto & [k, v] : statistics_keeper_store.entries.at(key)->data.column_stats)
        {
            res.emplace_back(k);
        }
        return res;
    }

    StatsCollection readSingleStats(const StatsTableIdentifier & table, const std::optional<String> & column_name_opt) override
    {
        auto & statistics_keeper_store = getStatisticsKeeperStore();
        std::shared_lock lck(statistics_keeper_store.mtx);
        auto key = table.getUniqueKey();

        if (!statistics_keeper_store.entries.contains(key))
        {
            return {};
        }

        auto & entry_data = statistics_keeper_store.entries.at(key)->data;

        if (!column_name_opt.has_value())
        {
            return entry_data.table_stats;
        }
        else
        {
            auto column_name = *column_name_opt;
            auto it = entry_data.column_stats.find(column_name);
            if (it != entry_data.column_stats.end())
            {
                return it->second;
            }
            else
            {
                return {};
            }
        }
    }

    
    void writeStatsData(const StatsTableIdentifier & table, const StatsData & stats_data) override
    {
        auto & statistics_keeper_store = getStatisticsKeeperStore();
        String full_table_name = backQuoteIfNeed(table.getDatabaseName()) + "." + backQuoteIfNeed(table.getTableName());
        String stats_data_str;
        {
            std::unique_lock lck(statistics_keeper_store.mtx);
            auto key = table.getUniqueKey();
            if (!statistics_keeper_store.entries.contains(key))
            {
                // create new instance
                auto new_entry = std::make_shared<TableEntry>(TableEntry{table, {}});
                statistics_keeper_store.entries.emplace(key, new_entry);
            }
            assert(statistics_keeper_store.entries.count(key));
            auto & target = statistics_keeper_store.entries.at(key)->data;

            if (!stats_data.table_stats.empty())
            {
                target.table_stats = stats_data.table_stats;
            }

            for (auto & [column_name, column_stats] : stats_data.column_stats)
            {
                if (!column_stats.empty())
                {
                    target.column_stats[column_name] = column_stats;
                }
            }
            stats_data_str = target.serialize(full_table_name);
        }

        statistics_keeper_store.updateTableStatisticsOnKeeper(table.getDatabaseName(), table.getTableName(), stats_data_str);
    }

    void dropStatsColumnData(const StatsTableIdentifier & table, const ColumnDescVector & cols_desc) override
    {
        auto & statistics_keeper_store = getStatisticsKeeperStore();
        String stats_data_str;
        String full_table_name = backQuoteIfNeed(table.getDatabaseName()) + "." + backQuoteIfNeed(table.getTableName());
        {
            std::unique_lock lck(statistics_keeper_store.mtx);
            auto key = table.getUniqueKey();
            if (statistics_keeper_store.entries.contains(key))
            {
                auto & entry = statistics_keeper_store.entries.at(key);
                if (entry)
                {
                    for (auto & col_desc : cols_desc)
                    {
                        entry->data.column_stats.erase(col_desc.name);
                    }
                    if (!entry->data.column_stats.empty() && !entry->data.table_stats.empty())
                    {
                        stats_data_str = entry->data.serialize(full_table_name);
                    }
                }
            }  
        }

        if (stats_data_str.empty())
            statistics_keeper_store.dropTableStatisticsOnKeeper(table.getDatabaseName(), table.getTableName());
        else
            statistics_keeper_store.updateTableStatisticsOnKeeper(table.getDatabaseName(), table.getTableName(), stats_data_str);
    }

    void dropStatsData(const StatsTableIdentifier & table) override
    {
        auto & statistics_keeper_store = getStatisticsKeeperStore();
        {
            std::unique_lock lck(statistics_keeper_store.mtx);
            auto key = table.getUniqueKey();
            statistics_keeper_store.entries.erase(key);
        }

        statistics_keeper_store.dropTableStatisticsOnKeeper(table.getDatabaseName(), table.getTableName());
    }


    std::vector<StatsTableIdentifier> getAllTablesID(const String & database_name) override
    {
        std::vector<StatsTableIdentifier> results;
        auto db = DatabaseCatalog::instance().getDatabase(database_name, context);
        for (auto iter = db->getTablesIterator(context); iter->isValid(); iter->next())
        {
            auto table = iter->table();
            if (!table)
                continue;
            StatsTableIdentifier table_id(table->getStorageID());
            if (!isTableCollectable(table_id))
                continue;

            results.emplace_back(table_id);
        }
        return results;
    }


    std::optional<StatsTableIdentifier> getTableIdByName(const String & database_name, const String & table_name) override
    {
        auto & ins = DatabaseCatalog::instance();
        auto db_storage = ins.getDatabase(database_name, context);
        auto table = db_storage->tryGetTable(table_name, context);
        if (!table)
        {
            return std::nullopt;
        }
        auto result = table->getStorageID();

        return StatsTableIdentifier(result);
    }

    std::optional<StatsTableIdentifier> getTableIdByUUID(const UUID & uuid) override
    {
        (void)uuid;
        // this should be called only in daemon manager
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unimplemented");
    }

    StoragePtr getStorageByTableId(const StatsTableIdentifier & identifier) override
    {
        auto & ins = DatabaseCatalog::instance();
        return ins.getTable(identifier.getStorageID(), context);
    }

    StoragePtr tryGetStorageByUUID(const UUID & uuid) override
    {
        (void)uuid;
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unimplemented");
    }


    UInt64 getUpdateTime() override
    {
        // never use
        return 0;
    }

private:
    StatisticsKeeperStore & getStatisticsKeeperStore() { return *statistics_keeper_store; }
    std::shared_ptr<StatisticsKeeperStore> statistics_keeper_store;
};

CatalogAdaptorPtr createCatalogAdaptorKeeper(ContextPtr query_context)
{
    if (query_context->hasSessionContext())
    {
        auto session_context = query_context->getSessionContext();
        auto statistics_keeper_store = session_context->getOptimizerContext()->getStatisticsKeeperStore();
        return std::make_shared<CatalogAdaptorKeeper>(query_context, statistics_keeper_store);
    }
    else
    {
        // for test environment
        static auto statistics_keeper_store = std::make_shared<StatisticsKeeperStore>(query_context);
        return std::make_shared<CatalogAdaptorKeeper>(query_context, statistics_keeper_store);
    }
}
}
