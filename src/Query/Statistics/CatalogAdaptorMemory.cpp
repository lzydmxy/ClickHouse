#include <Query/Statistics/CatalogAdaptor.h>

//#include <Statistics/CacheManager.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Query/Statistics/SerdeUtils.h>
//#include <Query/Statistics/StatisticsCollector.h>
#include <Query/Statistics/StatisticsMemoryStore.h>
#include <Query/Statistics/TypeUtils.h>
#include <fmt/format.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <string>

namespace DB::QueryStatistics
{

class CatalogAdaptorMemory : public CatalogAdaptor
{
public:
    CatalogAdaptorMemory(ContextPtr context_, std::shared_ptr<StatisticsMemoryStore> sms_ptr)
        : CatalogAdaptor(context_), statistics_memory_store(sms_ptr)
    {
    }

    bool hasStatsData(const StatsTableIdentifier & table) override
    {
        auto & sms = getStatisticsMemoryStore();
        std::shared_lock lck(sms.mtx);
        auto key = table.getUniqueKey();
        /// return whether table_stats of the corresponding table is non-empty
        return sms.entries.count(key);
    }

    StatsData readStatsData(const StatsTableIdentifier & table) override
    {
        auto & sms = getStatisticsMemoryStore();
        std::shared_lock lck(sms.mtx);
        auto key = table.getUniqueKey();

        if (!sms.entries.count(key))
        {
            return {};
        }

        return sms.entries.at(key)->data;
    }


    std::vector<String> readStatsColumnsKey(const StatsTableIdentifier & table) override
    {
        std::vector<String> res;

        auto & sms = getStatisticsMemoryStore();
        std::shared_lock lck(sms.mtx);
        auto key = table.getUniqueKey();

        if (!sms.entries.count(key))
        {
            return {};
        }

        for (auto & [k, v] : sms.entries.at(key)->data.column_stats)
        {
            res.emplace_back(k);
        }
        return res;
    }

    StatsCollection readSingleStats(const StatsTableIdentifier & table, const std::optional<String> & column_name_opt) override
    {
        auto & sms = getStatisticsMemoryStore();
        std::shared_lock lck(sms.mtx);
        auto key = table.getUniqueKey();

        if (!sms.entries.count(key))
        {
            return {};
        }

        auto & entry_data = sms.entries.at(key)->data;

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


    // note: new
    void writeStatsData(const StatsTableIdentifier & table, const StatsData & stats_data) override
    {
        // meta.getEntry(table.getUniqueKey())->data = stats_data;

        auto & sms = getStatisticsMemoryStore();
        std::unique_lock lck(sms.mtx);
        auto key = table.getUniqueKey();
        if (!sms.entries.count(key))
        {
            // create new instance
            auto new_entry = std::make_shared<TableEntry>(TableEntry{table, {}});
            sms.entries.emplace(key, new_entry);
        }
        assert(sms.entries.count(key));
        auto & target = sms.entries.at(key)->data;

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
    }

    void dropStatsColumnData(const StatsTableIdentifier & table, const ColumnDescVector & cols_desc) override
    {
        auto & sms = getStatisticsMemoryStore();
        std::unique_lock lck(sms.mtx);
        auto key = table.getUniqueKey();
        if (sms.entries.count(key))
        {
            auto & entry = sms.entries.at(key);
            if (!entry)
                return;
            for (auto & col_desc : cols_desc)
            {
                entry->data.column_stats.erase(col_desc.name);
            }
        }
    }

    void dropStatsData(const StatsTableIdentifier & table) override
    {
        auto & sms = getStatisticsMemoryStore();
        std::unique_lock lck(sms.mtx);
        auto key = table.getUniqueKey();
        sms.entries.erase(key);
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
    StatisticsMemoryStore & getStatisticsMemoryStore() { return *statistics_memory_store; }
    std::shared_ptr<StatisticsMemoryStore> statistics_memory_store;
};

CatalogAdaptorPtr createCatalogAdaptorMemory(ContextPtr query_context)
{
    if (query_context->hasSessionContext())
    {
        auto session_context = query_context->getSessionContext();
        auto sms = session_context->getOptimizerContext()->getStatisticsMemoryStore();
        return std::make_shared<CatalogAdaptorMemory>(query_context, sms);
    }
    else
    {
        // for test environment
        static auto sms_static = std::make_shared<StatisticsMemoryStore>();
        return std::make_shared<CatalogAdaptorMemory>(query_context, sms_static);
    }
}
}
