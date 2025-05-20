#include <Query/Statistics/StatsColumnBasic.h>

#include <Query/Statistics/SerdeUtils.h>
#include <google/protobuf/util/json_util.h>

namespace DB::QueryStatistics
{
    String StatsColumnBasic::serialize() const
    {
        return proto.SerializeAsString();
    }
    void StatsColumnBasic::deserialize(std::string_view blob)
    {
        ASSERT_PARSE(proto.ParseFromArray(blob.data(), blob.size()));
    }
    String StatsColumnBasic::serializeToJson() const
    {
        String json_str;
        auto status = google::protobuf::util::MessageToJsonString(proto, &json_str);
        if (!status.ok())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to convert QueryPlan to json: {}", status.message());
        return json_str;
    }
    void StatsColumnBasic::deserializeFromJson(std::string_view json)
    {
        auto status = google::protobuf::util::JsonStringToMessage(std::string(json), &proto);
        if (!status.ok())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to convert QueryPlan to json: {}", status.message());
    }

} // namespace DB
