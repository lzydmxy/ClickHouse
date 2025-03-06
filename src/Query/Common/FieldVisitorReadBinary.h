#pragma once
#include <Common/FieldVisitors.h>

namespace DB
{

template <typename F>
Field dispatchField(F && f, Field::Types::Which type);

/// Refer src/Common/FieldVisitorWriteBinary.h
class FieldVisitorReadBinary
{
public:
private:
    ReadBuffer & buf;

public:
    explicit FieldVisitorReadBinary(ReadBuffer & buf_) : buf(buf_)
    {
    }

    template <typename RealType>
    Field operator()()
    {
        if constexpr (std::is_same_v<RealType, Null>)
        {
            return Field();
        }
        else if constexpr (std::is_same_v<RealType,Decimal32> || std::is_same_v<RealType, Decimal64>
            || std::is_same_v<RealType, Decimal128> || std::is_same_v<RealType, Decimal256>)
        {
            DecimalField<RealType> value;
            read(value, buf);
            return Field(value);
        }
        else
        {
            RealType value;
            read(value, buf);
            return Field(value);
        }
    }
private:
    static void read(UInt64 & value, ReadBuffer & buf);
    static void read(UInt128 & value, ReadBuffer & buf);
    static void read(UInt256 & value, ReadBuffer & buf);
    static void read(Int64 & value, ReadBuffer & buf);
    static void read(Int128 & value, ReadBuffer & buf);
    static void read(Int256 & value, ReadBuffer & buf);

    static void read(UUID & value, ReadBuffer & buf);
    static void read(IPv4 & value, ReadBuffer & buf);
    static void read(IPv6 & value, ReadBuffer & buf);
    static void read(Float64 & value, ReadBuffer & buf);
    static void read(String & value, ReadBuffer & buf);

    static void read(DecimalField<Decimal32> & value, ReadBuffer & buf);
    static void read(DecimalField<Decimal64> & value, ReadBuffer & buf);
    static void read(DecimalField<Decimal128> & value, ReadBuffer & buf);
    static void read(DecimalField<Decimal256> & value, ReadBuffer & buf);

    static void read(Array & value, ReadBuffer & buf);
    static void read(Tuple & value, ReadBuffer & buf);
    static void read(Map & value, ReadBuffer & buf);
    static void read(Object & value, ReadBuffer & buf);

    static void read(AggregateFunctionStateData & value, ReadBuffer & buf);
    static void read(CustomType & x, ReadBuffer & buf);
    static void read(bool & x, ReadBuffer & buf);
};

}
