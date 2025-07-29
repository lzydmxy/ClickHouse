#pragma once

#include <type_traits>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/binary_from_base64.hpp>

#include <AggregateFunctions/AggregateFunctionUniq.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <set>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Query/DataTypes/DataTypeSketchBinary.h>
#include <Query/Columns/ColumnSketchBinary.h>

namespace DB
{
/// DataTypeCtor Example:
// TODO: use concept
#if 0
template <typename T>
struct SetData
{
    using Self = SetData;
    std::set<T> sum_{};

    void add(T value)
    {
        // todo
        sum_.insert(value);
    }

    void merge(const Self & rhs)
    {
        // TODO
        auto cpy = rhs.sum_;
        this->sum_.merge(std::move(cpy));
    }

    void write(WriteBuffer & buf) const
    {
        int count = sum_.size();
        writeBinary(count, buf);
        for (auto x : sum_)
        {
            writeBinary(x, buf);
        }
    }

    void read(ReadBuffer & buf)
    {
        sum_.clear();
        int64_t count;
        readBinary(count, buf);
        while (count-- > 0)
        {
            T x;
            readBinary(x, buf);
            sum_.insert(x);
        }
    }

    std::string getConcat() const
    {
        std::string res;
        for (auto x : sum_)
        {
            res += fmt::format("{}", x);
        }
        return res;
    }

    void insertResultInto(IColumn & to) const
    {
        auto str = getConcat();
        static_cast<ColumnString &>(to).insertData(str.data(), str.size());
    }

    static String getName() { return "stats_set"; }
};
#endif

template <template <typename> typename DataTypeCtor, typename T, bool second_param = false>
class AggregateFunctionCboFamily;

template <typename T>
struct type_utils
{
    using EmbededType = T;
    using ColVecType = ColumnVector<T>;
};

/// statistics collector for cbo
/// for simplicity, convert all stats to base64 string as output
template <template <typename> typename DataTypeCtor, typename T, bool second_param>
class AggregateFunctionCboFamily final
    : public IAggregateFunctionDataHelper<DataTypeCtor<typename type_utils<T>::EmbededType>, AggregateFunctionCboFamily<DataTypeCtor, T, second_param>>
{
public:
    using Utils = type_utils<T>;
    using EmbededType = typename Utils::EmbededType;
    using Self = AggregateFunctionCboFamily;
    using Data = DataTypeCtor<EmbededType>;
    using ParentHelper = IAggregateFunctionDataHelper<Data, Self>;

    using ResultDataType = DataTypeSketchBinary;

    // TODO: support string and decimal
    using ColVecType = typename Utils::ColVecType;
    using ColVecResult = ColumnString;
    String getName() const override { return Data::getName(); }

    template <typename... Args>
    AggregateFunctionCboFamily(const DataTypes & argument_types_, Args... args) : ParentHelper(argument_types_, {}, createResultType()), init_data_(args...)
    {
    }

    template <typename... Args>
    AggregateFunctionCboFamily(const IDataType & data_type, const DataTypes & argument_types_, Args... args)
        : ParentHelper(argument_types_, {}, createResultType()), init_data_(args...)
    {
        (void)data_type;
    }

    static DataTypePtr createResultType() { return std::make_shared<ResultDataType>(); }

    void create(AggregateDataPtr place) const override
    {
        new (place) Data(init_data_);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = static_cast<const ColVecType &>(*columns[0]);
        auto value = column.getData()[row_num];
        if constexpr (!second_param)
        {
            this->data(place).add(value);
        }
        else
        {
            using SecondType = ColumnVector<UInt64>;
            const auto& column_second = static_cast<const SecondType&>(*columns[1]);
            auto value_second = column_second.getData()[row_num];
            this->data(place).add(value, value_second);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override { this->data(place).merge(this->data(rhs)); }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf, std::optional<size_t> /* version */) const override { this->data(place).write(buf); }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, std::optional<size_t> /* version  */, Arena *) const override { this->data(place).read(buf); }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        const auto & data = this->data(place);
        data.insertResultInto(to);
    }

    bool allocatesMemoryInArena() const override { return false; }

private:
    Data init_data_;
};

}
