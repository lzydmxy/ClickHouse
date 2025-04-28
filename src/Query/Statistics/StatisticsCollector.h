#pragma once

#include <Common/Logger.h>
#include <utility>
#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <Query/Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Query/Protos/optimizer_statistics.pb.h>
#include <Query/Statistics/Base64.h>
#include <Query/Statistics/CatalogAdaptor.h>
#include <Query/Statistics/CollectorSettings.h>
#include <Query/Statistics/StatisticsCollectorObjects.h>

namespace DB::QueryStatistics
{

// uuid level
class StatisticsCollector
{
public:
    friend class CollectStep;

    using TableStats = StatisticsImpl::TableStats;
    using ColumnStats = StatisticsImpl::ColumnStats;
    using ColumnStatsMap = StatisticsImpl::ColumnStatsMap;

    StatisticsCollector(
        ContextPtr context_, CatalogAdaptorPtr catalog_, const StatsTableIdentifier & table_info_, const CollectorSettings & settings_);

    void collect(const ColumnDescVector & col_names);

    void writeToCatalog();
    void readAllFromCatalog();
    void readFromCatalog(const std::vector<String> & cols_name);
    void readFromCatalogImpl(const ColumnDescVector & cols_desc);

    std::optional<PlanNodeStatisticsPtr> toPlanNodeStatistics() const;

    const auto & getTableStats() const { return table_stats; }
    const auto & getColumnsStats() const { return columns_stats; }
    void setTableStats(TableStats && stats) { table_stats = std::move(stats); }
    void setColumnStats(String col_name, ColumnStats && col_stats) { columns_stats[col_name] = std::move(col_stats); }

private:
    ContextPtr context;
    LoggerPtr logger;
    CatalogAdaptorPtr catalog;
    StatsTableIdentifier table_info;

    // table stats
    TableStats table_stats;
    StoragePtr storage;

    // column stats
    ColumnStatsMap columns_stats;
    CollectorSettings settings;
};
}
