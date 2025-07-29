#include <Query/AggregateFunctions/AggregateFunctionCboFamily.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <Query/Statistics/Base64.h>
#include <Query/Statistics/StatsHllSketch.h>

// TODO: use datasketches
namespace DB
{
struct CpcData
{
    QueryStatistics::StatsHllSketch data_;

    template <typename T>
    void add(T value)
    {
        data_.update(value);
    }

    void merge(const CpcData & rhs) { return data_.merge(rhs.data_); }

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

    // String getText() const { return data(); }

    void insertResultInto(IColumn & to) const
    {
        BlobType blob = data_.serialize();
        dynamic_cast<ColumnSketchBinary &>(to).insertData(blob.c_str(), blob.size());
    }

    static String getName() { return "hll"; }
};

template <typename T>
using CpcDataAdaptor = CpcData;

template <template <typename> class Function>
AggregateFunctionPtr
createAggregateFunctionHllSketch(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res;
    const DataTypePtr& data_type = argument_types[0];
    WhichDataType which(data_type);

    // TODO: support most data_type
    if (DB::isColumnedAsNumber(data_type))
    // if (which.isInt() || which.isUInt() || which.isFloat()
    //       || which.isDate() || which.isDate32() || which.isDateTime()
    //       || which.isDateTime64() || which.isUUID())
    {
        res.reset(createWithNumericBasedType<Function>(*data_type, argument_types));
    }

    if (!res)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of argument for aggregate function {}", argument_types[0]->getName(), name);
    }
    return res;
}

template <typename T>
struct FuncImpl
{
    using Func = AggregateFunctionCboFamily<CpcDataAdaptor, T>;
};
template <typename T>
using Func = typename FuncImpl<T>::Func;


void registerAggregateFunctionHllSketch(AggregateFunctionFactory & factory)
{
    AggregateFunctionWithProperties functor;
    functor.creator = createAggregateFunctionHllSketch<Func>;
    factory.registerFunction("hll", functor);
}

}
