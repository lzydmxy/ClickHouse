#include <Query/Statistics/CacheManager.h>
#include <Query/Statistics/CatalogAdaptor.h>
#include <Query/Statistics/Parameters.h>

#include <memory>

namespace DB::QueryStatistics
{

    std::unique_ptr<CacheManager::CacheType> CacheManager::cache;


    void CacheManager::initialize(ContextPtr context)
    {
        if (cache)
        {
            LOG_WARNING(getLogger("CacheManager"), "CacheManager already initialized");
            return;
        }
        auto max_size = context->getConfigRef().getUInt64("optimizer.statistics.max_cache_size", ConfigParameters::max_cache_size);

        auto expire_time = std::chrono::seconds(
            context->getConfigRef().getUInt64("optimizer.statistics.cache_expire_time", ConfigParameters::cache_expire_time));
        initialize(max_size, expire_time);
    }

    void CacheManager::initialize(UInt64 entry_size, std::chrono::seconds expire_time)
    {
        (void)entry_size;
        cache = std::make_unique<CacheType>(expire_time);
    }

    void CacheManager::reset()
    {
        cache->clear();
    }

    void CacheManager::invalidate(ContextPtr context, const StatsTableIdentifier & table)
    {
        (void)context;
        // this operation is lightweight:
        //     local, in memory and exception free
        if (cache)
            cache->invalidate(table.getUniqueKey(context));
    }
} // namespace DB::QueryStatistics
