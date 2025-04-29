#pragma once

#include <Interpreters/Context.h>
#include <Query/Optimizer/MaterializedView/MaterializedViewStructure.h>

namespace DB
{
class MaterializedViewMemoryCache
{
public:
    static MaterializedViewMemoryCache & instance();

    std::optional<MaterializedViewStructurePtr> getMaterializedViewStructure(
        const StorageID & database_and_table_name,
        ContextMutablePtr context,
        bool local_materialized_view,
        const std::map<String, StorageID> & local_table_to_distributed_table);


private:
    MaterializedViewMemoryCache() = default;

    std::optional<StorageID> findTargetTable(
        bool local_materialized_view, const StorageMaterializedView & view, const StorageID & view_id, ContextMutablePtr context);

    // todo: implement lru cache.
    // std::unordered_map<String, MaterializedViewStructurePtr> materialized_view_structures;
    // mutable std::shared_mutex mutex;

    class LocalTableRewriter;
};
}
