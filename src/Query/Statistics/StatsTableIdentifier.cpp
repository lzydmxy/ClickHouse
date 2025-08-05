#include <Query/Statistics/StatsTableIdentifier.h>
#include <Storages/IStorage.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/StorageDistributed.h>


namespace DB::QueryStatistics
{
auto StatsTableIdentifier::getUniqueKey(ContextPtr context) const -> UUID
{
    auto uuid = getUUID();
    if (uuid != UUID{})
    {
        return uuid;
    }

    auto database = getDatabaseName();
    auto table = getTableName();

    // This approach involves some tricks, but it allows us to centralize modifications here.
    // For distributed tables, we use the local table name to generate the UUID.
    // So that the local table can use distributed tables statistics data.
    if (auto storage = DatabaseCatalog::instance().tryGetTable(storage_id, context))
    {
        if (auto dis_storage = std::dynamic_pointer_cast<StorageDistributed>(storage))
        {
            database = dis_storage->getRemoteDatabaseName();
            table = dis_storage->getRemoteTableName();
        }
    }

    auto hash_db = std::hash<String>()(database);
    auto hash_tb = std::hash<String>()(table);
    hash_db = (hash_db & 0xffffffffffff0fffull) | 0x0000000000004000ull;
    hash_tb = (hash_tb & 0x3fffffffffffffffull) | 0x8000000000000000ull;

    uuid.toUnderType().items[0] = hash_db;
    uuid.toUnderType().items[1] = hash_tb;
    return uuid;
}

}
