#include <Query/Statistics/StatisticsBase.h>
namespace DB::QueryStatistics
{

String StatsData::serialize(std::string_view name)
{
    Protos::TableStats proto;
    *proto.mutable_table_name() = name;
    for (const auto & [tag , stats] : table_stats)
    {
        proto.mutable_blobs()->insert({static_cast<int64_t>(tag), stats->serialize()});
    }

    for (const auto & [column_name , col_stats] : column_stats)
    {
        auto & columns_proto = *proto.add_columns();
        *columns_proto.mutable_column_name() = column_name;
        for (const auto & [col_tag , stats] : col_stats)
        {
            columns_proto.mutable_blobs()->insert({static_cast<int64_t>(col_tag), stats->serialize()});
        }
    }
    return proto.SerializeAsString();
}

std::pair<String, StatsData> StatsData::deserialize(std::string_view blob)
{
    Protos::TableStats proto;
    proto.ParseFromArray(blob.data(), static_cast<int>(blob.size()));
    String table_name = proto.table_name();

    StatsCollection table_stats;
    for (const auto & [tag , stats] : proto.blobs())
    {
        table_stats.emplace(static_cast<StatisticsTag>(tag), createStatisticsBase(static_cast<StatisticsTag>(tag), stats));
    }

    std::unordered_map<String, StatsCollection> column_stats;
    for (const auto & columns : proto.columns())
    {
        StatsCollection col_stats;
        for (const auto & [tag , stats] : columns.blobs())
        {
            col_stats.emplace(static_cast<StatisticsTag>(tag), createStatisticsBase(static_cast<StatisticsTag>(tag), stats));
        }

        column_stats.emplace(columns.column_name(), col_stats);
    }

    return {proto.table_name(), StatsData{table_stats, column_stats}};
}

}
