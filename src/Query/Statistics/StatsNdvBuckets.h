#pragma once

#include <DataTypes/IDataType.h>
#include <Query/Statistics/StatisticsBase.h>
#include <Common/Exception.h>
#include <Query/Protos/optimizer_statistics.pb.h>
#include <Query/Statistics/Base64.h>
#include <Query/Statistics/SerdeDataType.h>
#include <Query/Statistics/StatsNdvBucketsResult.h>


namespace DB::QueryStatistics
{
template <typename T>
class StatsNdvBucketsImpl;
template <typename T>
class StatsNdvBucketsResultImpl;
class BucketBounds;

class StatsNdvBuckets : public StatisticsBase
{
public:
    static constexpr auto tag = StatisticsTag::NdvBuckets;

    StatisticsTag getTag() const override { return tag; }

    virtual SerdeDataType getSerdeDataType() const = 0;

    template <typename T>
    using Impl = StatsNdvBucketsImpl<T>;

    virtual const BucketBounds & getBucketBounds() const = 0;
    virtual std::shared_ptr<StatsNdvBucketsResult> asResult() const = 0;
};
}
