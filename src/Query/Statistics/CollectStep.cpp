#include <Query/Statistics/CollectStep.h>

#include <Query/Statistics/CommonErrorCodes.h>
#include <Query/Statistics/StatisticsCollector.h>

namespace DB::QueryStatistics
{

CollectStep::CollectStep(StatisticsCollector & core_)
    : core(core_), table_info(core_.table_info), catalog(core_.catalog), context(core_.context), handler_context(core_.settings)
{
}


void CollectStep::writeResult(TableStats & core_table_stats, ColumnStatsMap & core_columns_stats)
{
    auto table_basic = std::make_shared<StatsTableBasic>();
    table_basic->setRowCount(std::llround(handler_context.full_count));
    core_table_stats.basic = table_basic;

    for (auto & [col_name, col_data] : handler_context.columns_data)
    {
        ColumnStats column_stats;
        column_stats.basic = std::make_shared<StatsColumnBasic>();
        column_stats.basic->mutableProto().set_min_as_double(col_data.min_as_double);
        column_stats.basic->mutableProto().set_max_as_double(col_data.max_as_double);
        column_stats.basic->mutableProto().set_nonnull_count(std::llround(col_data.nonnull_count));

        auto ndv_value_regulated = std::min<double>(std::llround(col_data.nonnull_count), col_data.ndv_value);
        column_stats.basic->mutableProto().set_ndv_value(ndv_value_regulated);
        if (col_data.length_opt)
            column_stats.basic->mutableProto().set_total_length(col_data.length_opt.value());

        if (col_data.ndv_buckets_result_opt.has_value())
        {
            column_stats.ndv_buckets_result = col_data.ndv_buckets_result_opt.value();
        }

        core_columns_stats.emplace(col_name, std::move(column_stats));
    }
}
void CollectStep::collectTable()
{
    // try get count by fast trivial count
    if (auto count_opt = catalog->queryRowCount(table_info))
    {
        handler_context.full_count = count_opt.value();
        return;
    }

    TableHandler table_handler(table_info);
    table_handler.registerHandler(std::make_unique<RowCountHandler>(handler_context));
    //  select count(*) from <table>;
    auto sql = table_handler.getFullSql();
    auto query_context = SubqueryHelper::createQueryContext(context);
    auto block = executeSubQueryWithOneRow(sql, query_context, false, false);
    table_handler.parse(block);
    handler_context.full_count = handler_context.query_row_count.value();
    handler_context.query_row_count = std::nullopt;
}


std::vector<ColumnDescVector> split(const ColumnDescVector & origin, UInt64 max_columns)
{
    std::vector<ColumnDescVector> result;
    ColumnDescVector current;
    for (auto & col_desc : origin)
    {
        if (current.size() >= max_columns)
        {
            result.emplace_back(std::move(current));
            current.clear();
        }
        current.emplace_back(col_desc);
    }
    if (!current.empty())
    {
        result.emplace_back(std::move(current));
    }
    return result;
}

}
