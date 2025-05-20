#pragma once

#include <Query/Statistics/StatisticsBase.h>
#include <Query/Protos/optimizer_statistics.pb.h>
#include <Common/Exception.h>

namespace DB::QueryStatistics
{
    // basic statistics at column level
    // a wrapper of Protos::ColumnBasic
    // currently just row_count
    class StatsColumnBasic : public StatisticsBase
    {
    public:
        static constexpr auto tag = StatisticsTag::ColumnBasic;
        StatsColumnBasic() = default;
        String serialize() const override;
        void deserialize(std::string_view blob) override;
        StatisticsTag getTag() const override { return tag; }

        Protos::StatsColumnBasic & mutableProto() { return proto; }
        const Protos::StatsColumnBasic & getProto() const { return proto; }

        String serializeToJson() const override;
        void deserializeFromJson(std::string_view json) override;

    private:
        Protos::StatsColumnBasic proto;
    };
}
