#pragma once

#include <Query/Statistics/CatalogAdaptor.h>
#include <Query/Statistics/StatisticsBase.h>
#include <Query/Statistics/StatsTableIdentifier.h>

namespace DB::QueryStatistics
{
    class CatalogAdaptorProxy
    {
    public:
        virtual ~CatalogAdaptorProxy() = default;

        virtual void put(const StatsTableIdentifier & table_id, StatsData && data) = 0;
        virtual StatsData get(const StatsTableIdentifier & table_id) = 0;
        virtual StatsData get(const StatsTableIdentifier & table_id, bool table_info, const ColumnDescVector & columns) = 0;

        virtual void drop(const StatsTableIdentifier & table_id) = 0;
        virtual void dropColumns(const StatsTableIdentifier & table_id, const ColumnDescVector & cols_desc) = 0;
    };

    using CatalogAdaptorProxyPtr = std::unique_ptr<CatalogAdaptorProxy>;

    CatalogAdaptorProxyPtr createCatalogAdaptorProxy(const CatalogAdaptorPtr & catalog, StatisticsCachePolicy policy);

}
