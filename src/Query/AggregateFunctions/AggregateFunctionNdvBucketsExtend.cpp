#include <Query/AggregateFunctions/AggregateFunctionCboFamily.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Query/AggregateFunctions/AggregateFunctionHelpers.h>
#include <Query/Statistics/Base64.h>
#include <Query/Statistics/StatisticsBaseImpl.h>
#include <Query/Statistics/StatsNdvBucketsExtendImpl.h>
#include <Common/FieldVisitors.h>
#include <Common/FieldVisitorToString.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TYPE_MISMATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

// This agg fucntion is to calculate Ndvs in Histogram
// it takes BucketBuckets as a paratmeter
// and build histogram using StatsNdvBuckets

// extend version will receive two arguments
// first is <col>
// second is hash(<col>, _mark_id)
template <typename T>
struct NdvBucketsExtendData
{
    // UUID is in fact UInt128, use UInt128 for calculation
    using EmbeddedType = std::conditional_t<std::is_same_v<T, UUID>, UInt128, T>;
    // Extend version: add block_ndvs
    QueryStatistics::StatsNdvBucketsExtendImpl<EmbeddedType> data_;

    NdvBucketsExtendData() = default;

    NdvBucketsExtendData(std::string_view blob)
    {
        QueryStatistics::BucketBoundsImpl<EmbeddedType> bounds;
        bounds.deserialize(blob);
        data_.initialize(std::move(bounds));
    }

    void add(T value, UInt64 block_value) { data_.update(value, block_value); }

    void merge(const NdvBucketsExtendData & rhs) { data_.merge(rhs.data_); }

    using BlobType = String;
    void write(WriteBuffer & buf) const
    {
        BlobType blob = data_.serialize();
        writeBinary(blob, buf);
    }

    void read(ReadBuffer & buf)
    {
        BlobType blob;
        readBinary(blob, buf);
        data_.deserialize(blob);
    }

    std::string getText() const { return data_.toString(); }

    void insertResultInto(IColumn & to) const
    {
        auto blob = data_.serialize();
        static_cast<ColumnString &>(to).insertData(blob.c_str(), blob.size());
    }

    static String getName() { return "ndv_buckets_extend"; }
};


template <template <typename> class Function>
AggregateFunctionPtr
createAggregateFunctionNdvBucketsExtend(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings*)
{
    if (parameters.empty())
    {
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "params mismatch");
    }

    assertBinary(name, argument_types);
    if (argument_types[1]->getTypeId() != TypeIndex::UInt64)
    {
        throw Exception(ErrorCodes::TYPE_MISMATCH, "The second type is required to be UInt64");
    }

    auto blob_b64 = applyVisitor(FieldVisitorToString(), parameters[0]);
    blob_b64 = [](std::string_view view) -> std::string_view {
        // trim '\'
        if (view.size() < 2)
            return {};
        else if (view.front() == '\'' && view.back() == '\'')
        {
            return view.substr(1, view.size() - 2);
        }
        else
        {
            return view;
        }
    }(blob_b64);
    auto blob = QueryStatistics::base64Decode(blob_b64);

    AggregateFunctionPtr res;
    DataTypePtr data_type = argument_types[0];

     WhichDataType which(data_type);
    if (which.isInt() || which.isUInt() || which.isFloat()
          || which.isDate() || which.isDate32() || which.isDateTime()
          || which.isDateTime64() || which.isUUID())
    {
        res.reset(createWithNumericTypeOrDateOrDateTime<Function>(*data_type, argument_types, blob));
    }

    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, 
            "Illegal type {} of argument for aggregate function {}", argument_types[0]->getName(), name);
    return res;
}

template <typename T>
struct FuncImpl
{
    using Func = AggregateFunctionCboFamily<NdvBucketsExtendData, T, true>;
};
template <typename T>
using Func = typename FuncImpl<T>::Func;


void registerAggregateFunctionNdvBucketsExtend(AggregateFunctionFactory & factory)
{
    AggregateFunctionWithProperties functor;
    functor.creator = createAggregateFunctionNdvBucketsExtend<Func>;
    factory.registerFunction("ndv_buckets_extend", functor);
}

}
