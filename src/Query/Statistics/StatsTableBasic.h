#pragma once

#include <Query/Statistics/StatisticsBase.h>

#include <Query/Protos/optimizer_statistics.pb.h>
#include <Common/Exception.h>

namespace DB::QueryStatistics
{
    // basic statistics at table level
    // a wrapper of Protos::TableBasic
    // currently just row_count
    class StatsTableBasic : public StatisticsBase
    {
    public:
        static constexpr auto tag = StatisticsTag::TableBasic;
        StatsTableBasic() = default;
        String serialize() const override;
        void deserialize(std::string_view blob) override;
        StatisticsTag getTag() const override { return tag; }

        void setRowCount(int64_t row_count);
        int64_t getRowCount() const;

        void setTimestamp(DateTime64 timestamp);
        DateTime64 getTimestamp() const;

        String serializeToJson() const override;
        void deserializeFromJson(std::string_view json) override;

    private:
        Protos::StatsTableBasic table_basic_pb;
    };

}
