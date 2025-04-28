#pragma once

#include <Query/Statistics/StatisticsBase.h>
#include <IO/WriteHelpers.h>
#include <Query/Optimizer/CardinalityEstimate/SymbolStatistics.h>
#include <Query/Protos/optimizer_statistics.pb.h>
#include <Query/Statistics/Base64.h>
#include <Query/Statistics/BucketBounds.h>
#include <Query/Statistics/SerdeUtils.h>

#include <algorithm>

namespace DB::QueryStatistics
{
template <typename T>
class StatsNdvBucketsResultImpl;

class StatsNdvBucketsResult : public StatisticsBase
{
public:
    static constexpr auto tag = StatisticsTag::NdvBucketsResult;

    StatisticsTag getTag() const override { return tag; }

    virtual SerdeDataType getSerdeDataType() const = 0;

    template <typename T>
    using Impl = StatsNdvBucketsResultImpl<T>;

    // todo: bc, use visitor pattern to hide implementations
    virtual void writeSymbolStatistics(SymbolStatistics & symbol) = 0;
    virtual const BucketBounds & getBucketBounds() const = 0;

    virtual size_t numBuckets() const = 0;

    static std::shared_ptr<StatsNdvBucketsResult> create(const BucketBounds & bounds, std::vector<UInt64> counts, std::vector<double> ndvs);

};


} // namespace DB
