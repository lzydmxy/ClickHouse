#pragma once

#include <Core/Types.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/Context_fwd.h>

// this can be very different for cnch/stable_v2
namespace DB::QueryStatistics
{

class StatsTableIdentifier
{
public:
    using UniqueKey = UUID;
    explicit StatsTableIdentifier(StorageID storage_id_) : storage_id(storage_id_) { }
    StatsTableIdentifier(const StatsTableIdentifier &) = default;
    StatsTableIdentifier(StatsTableIdentifier &&) = default;
    StatsTableIdentifier & operator=(const StatsTableIdentifier &) = default;
    StatsTableIdentifier & operator=(StatsTableIdentifier &&) = default;
    const String & getDatabaseName() const { return storage_id.database_name; }
    const String & getTableName() const { return storage_id.table_name; }

    String getDbTableName() const { return storage_id.getFullTableName(); }
    UniqueKey getUniqueKey(ContextPtr local_context) const;

    const StorageID & getStorageID() const { return storage_id; }
    StorageID & getMutableStorageID() { return storage_id; }
    UUID getUUID() const { return UUID{}; }
    String getNameForLogs() const { return storage_id.getNameForLogs(); }

    bool operator==(const StatsTableIdentifier & right) const { return storage_id == right.storage_id; }

private:
    StorageID storage_id;
    // useful only for adaptor
};
}

namespace std
{
template <>
struct hash<DB::QueryStatistics::StatsTableIdentifier>
{
    size_t operator()(const DB::QueryStatistics::StatsTableIdentifier & identifier) const { return std::hash<DB::UUID>{}(identifier.getUUID()); }
};

}
