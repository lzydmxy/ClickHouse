#pragma once

#include <Query/Protos/optimizer_statistics.pb.h>
#include <Query/Statistics/StatisticsBase.h>

namespace DB
{

class ProtoEnumUtils
{
public:
    static Protos::SerdeDataType serdeDataTypeFromString(const String & str)
    {
        return static_cast<Protos::SerdeDataType>(
            google::protobuf::GetEnumDescriptor<Protos::SerdeDataType>()->FindValueByName(str)->number());
    }

    static String serdeDataTypeToString(Protos::SerdeDataType data_type)
    {
        return google::protobuf::GetEnumDescriptor<Protos::SerdeDataType>()->FindValueByNumber(data_type)->name();
    }

    static QueryStatistics::StatisticsTag statisticsTagFromString(const String & str)
    {
        Protos::StatisticsType statistics_type = static_cast<Protos::StatisticsType>(
            google::protobuf::GetEnumDescriptor<Protos::StatisticsType>()->FindValueByName(str)->number());
        return static_cast<QueryStatistics::StatisticsTag>(statistics_type);
    }

    static String statisticsTagToString(QueryStatistics::StatisticsTag tag)
    {
        Protos::StatisticsType statistics_type = static_cast<Protos::StatisticsType>(tag);
        return google::protobuf::GetEnumDescriptor<Protos::StatisticsType>()->FindValueByNumber(statistics_type)->name();
    }
};
}
