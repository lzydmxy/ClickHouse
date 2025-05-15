#include <Interpreters/Context.h>
#include <Query/Statistics/CatalogAdaptorProxy.h>
#include <Query/Statistics/CatalogAdaptor.h>
#include <Query/Statistics/StatsTableBasic.h>
#include <Query/Statistics/StatsTableIdentifier.h>
#include <Query/Statistics/VersionHelper.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>

namespace DB::QueryStatistics
{
// this can be nullable
std::shared_ptr<StatsTableBasic> getTableStatistics(ContextPtr context, const StatsTableIdentifier & table)
{
    auto catalog = createCatalogAdaptor(context);
    auto policy = context->getOptimizerContext()->getSettingsRef().statistics_cache_policy;
    auto proxy = createCatalogAdaptorProxy(catalog, policy);
    auto stats = proxy->get(table, true, {});
    auto tag = StatisticsTag::TableBasic;
    if (auto iter = stats.table_stats.find(tag); iter != stats.table_stats.end())
    {
        auto obj = std::dynamic_pointer_cast<StatsTableBasic>(iter->second);
        return obj;
    }
    return nullptr;
}

std::optional<DateTime64> getVersion(ContextPtr context, const StatsTableIdentifier & table)
{
    try
    {
        auto obj = getTableStatistics(context, table);
        if (!obj)
            return std::nullopt;
        return obj->getTimestamp();
    }
    catch (...)
    {
        tryLogCurrentException(getLogger("QueryStatistics::getVersion"));
        return std::nullopt;
    }
}

}
