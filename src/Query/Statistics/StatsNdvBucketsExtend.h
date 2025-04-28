#pragma once

#include <DataTypes/IDataType.h>
#include <Query/Statistics/StatisticsBase.h>
#include <Common/Exception.h>

#include <Query/Optimizer/CardinalityEstimate/SymbolStatistics.h>
#include <Query/Protos/optimizer_statistics.pb.h>
#include <Query/Statistics/Base64.h>
#include <Query/Statistics/StatsKllSketch.h>
#include <Query/Statistics/StatsNdvBucketsResult.h>


namespace DB::QueryStatistics
{
    template <typename T>
    class StatsNdvBucketsExtendImpl;
    template <typename T>
    class StatsNdvBucketsResultImpl;
    class BucketBounds;

    class StatsNdvBucketsExtend : public StatisticsBase
    {
    public:
        static constexpr auto tag = StatisticsTag::NdvBucketsExtend;

        StatisticsTag getTag() const override { return tag; }

        virtual SerdeDataType getSerdeDataType() const = 0;

        template <typename T>
        using Impl = StatsNdvBucketsExtendImpl<T>;

        virtual const BucketBounds & getBucketBounds() const = 0;

        virtual std::vector<UInt64> getCounts() const = 0;
        virtual std::vector<double> getNdvs() const = 0;
        virtual std::vector<double> getBlockNdvs() const = 0;
    };


}
