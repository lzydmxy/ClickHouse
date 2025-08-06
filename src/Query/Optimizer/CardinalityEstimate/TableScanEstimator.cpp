#include <Query/Optimizer/CardinalityEstimate/TableScanEstimator.h>
#include <Query/Statistics/StatisticsCollector.h>
#include <Common/ErrorHandlers.h>
#include <Query/Optimizer/CardinalityEstimate/LimitEstimator.h>
#include <Interpreters/convertFieldToType.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
}

PlanNodeStatisticsPtr TableScanEstimator::estimate(ContextMutablePtr context, const TableScanStepExt & step)
{
    auto plan_node_stats_opt = estimate(context, step.getStorageID(), step.getColumnNames());
    if (!plan_node_stats_opt.has_value())
    {
        return nullptr;
    }
    auto plan_node_stats = std::move(plan_node_stats_opt.value());

    NameToNameMap alias_to_column;
    for (const auto & item : step.getColumnAlias())
    {
        alias_to_column[item.second] = item.first;
    }

    for (const auto & col : step.getOutputStream().header)
    {
        String column = col.name;
        if (alias_to_column.contains(col.name) && col.name != alias_to_column[col.name]
            && plan_node_stats->getSymbolStatistics().contains(alias_to_column[col.name]))
        {
            // rename
            plan_node_stats->getSymbolStatistics()[col.name] = plan_node_stats->getSymbolStatistics()[alias_to_column[col.name]];
            plan_node_stats->getSymbolStatistics().erase(alias_to_column[col.name]);
        }
        if (plan_node_stats->getSymbolStatistics().contains(col.name))
        {
            plan_node_stats->getSymbolStatistics()[col.name]->setType(col.type);
            plan_node_stats->getSymbolStatistics()[col.name]->setDbTableColumn(
                step.getDatabase() + "-" + step.getTable() + "-" + alias_to_column[col.name]);
        }
    }

    auto query_info = step.getQueryInfo();
    auto *query = query_info.query->as<ASTSelectQuery>();
    if (step.hasLimit() && query->limitLength())
    {
        Field converted = convertFieldToType(query->refLimitLength()->as<ASTLiteral>()->value, DataTypeUInt64());
        return LimitEstimator::getLimitStatistics(plan_node_stats, converted.safeGet<UInt64>());
    }

    return plan_node_stats;
}

std::optional<PlanNodeStatisticsPtr> TableScanEstimator::estimate(
    ContextMutablePtr context, const StorageID & storage_id, const Names & columns)
{
    if (storage_id.getDatabaseName() == "system" || storage_id.getDatabaseName() == "_table_function")
    {
        return std::nullopt;
    }

    auto catalog = QueryStatistics::createCatalogAdaptor(context);
    auto table_info_opt = catalog->getTableIdByName(storage_id.getDatabaseName(), storage_id.getTableName());
    if (!table_info_opt.has_value())
    {
        // TODO: give a warning here?
        return std::nullopt;
    }

    PlanNodeStatisticsPtr plan_node_stats;
    try {
        QueryStatistics::StatisticsCollector collector(context, catalog, table_info_opt.value(), {});
        collector.readFromCatalog(columns);
        auto plan_node_stats_opt = collector.toPlanNodeStatistics();
        if (!plan_node_stats_opt.has_value())
        {
            return std::nullopt;
        }
        plan_node_stats = std::move(plan_node_stats_opt.value());
    }
    catch(...)
    {
        auto logger = getLogger("TableScanEstimator");
        tryLogCurrentException(logger);
        return std::nullopt;
    }

    return plan_node_stats;
}

}
