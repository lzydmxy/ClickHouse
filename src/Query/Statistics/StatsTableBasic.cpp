#include <Query/Statistics/StatsTableBasic.h>
#include <google/protobuf/util/json_util.h>

namespace DB::QueryStatistics
{
    String StatsTableBasic::serialize() const
    {
        return table_basic_pb.SerializeAsString();
    }
    void StatsTableBasic::deserialize(std::string_view blob)
    {
        table_basic_pb.ParseFromArray(blob.data(), static_cast<int>(blob.size()));
    }
    void StatsTableBasic::setRowCount(int64_t row_count)
    {
        table_basic_pb.set_row_count(row_count);
    }
    int64_t StatsTableBasic::getRowCount() const
    {
        return table_basic_pb.row_count();
    }

    void StatsTableBasic::setTimestamp(DateTime64 timestamp)
    {
        table_basic_pb.set_timestamp(timestamp.value);
    }

    DateTime64 StatsTableBasic::getTimestamp() const
    {
        DateTime64 result;
        result.value = table_basic_pb.has_timestamp() ? table_basic_pb.timestamp() : 0;
        return result;
    }

    String StatsTableBasic::serializeToJson() const
    {
        String json_str;
        auto status = google::protobuf::util::MessageToJsonString(table_basic_pb, &json_str);
        if (!status.ok())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to serialize to JSON: {}", status.message());
        return json_str;
    }
    void StatsTableBasic::deserializeFromJson(std::string_view json)
    {
        auto status = google::protobuf::util::JsonStringToMessage(std::string(json), &table_basic_pb);
        if (!status.ok())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to deserialize from JSON: {}", status.message());
    }

}
