#include "FieldVisitorReadBinary.h"
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>

namespace DB
{

/// Do some conversion or deserialization work base on type
template <typename F>
Field dispatchField(F && f, Field::Types::Which type)
{
    switch (type)
    {
        case Field::Types::Null:
            return f.template operator()<Null>();
        case Field::Types::UInt64:
            return f.template operator()<UInt64>();
        case Field::Types::UInt128:
            return f.template operator()<UInt128>();
        case Field::Types::UInt256:
            return f.template operator()<UInt256>();
        case Field::Types::Int64:
            return f.template operator()<Int64>();
        case Field::Types::Int128:
            return f.template operator()<Int128>();
        case Field::Types::Int256:
            return f.template operator()<Int256>();
        case Field::Types::UUID:
            return f.template operator()<UUID>();
        case Field::Types::Float64:
            return f.template operator()<Float64>();
        case Field::Types::String:
            return f.template operator()<String>();
        case Field::Types::Array:
            return f.template operator()<Array>();
        case Field::Types::Tuple:
            return f.template operator()<Tuple>();
        case Field::Types::Map:
            return f.template operator()<Map>();
        case Field::Types::Decimal32:
            return f.template operator()<Decimal32>();
        case Field::Types::Decimal64:
            return f.template operator()<Decimal64>();
        case Field::Types::Decimal128:
            return f.template operator()<Decimal128>();
        case Field::Types::Decimal256:
            return f.template operator()<Decimal256>();
        case Field::Types::AggregateFunctionState:
            return f.template operator()<AggregateFunctionStateData>();
        case Field::Types::Object:
            return f.template operator()<Object>();
        case Field::Types::IPv4:
            return f.template operator()<IPv4>();
        case Field::Types::IPv6:
            return f.template operator()<IPv6>();
        case Field::Types::CustomType:
            return f.template operator()<Object>();
        case Field::Types::Bool:
            return f.template operator()<bool>();
        case Field::Types::SketchBinary:
            __builtin_unreachable();
    }
    __builtin_unreachable();
}

void FieldVisitorReadBinary::read(UInt64 & value, ReadBuffer & buf) { readBinary(value, buf); }
void FieldVisitorReadBinary::read(UInt128 & value, ReadBuffer & buf) { readBinary(value, buf); }
void FieldVisitorReadBinary::read(UInt256 & value, ReadBuffer & buf) { readBinary(value, buf); }
void FieldVisitorReadBinary::read(Int64 & value, ReadBuffer & buf) { readBinary(value, buf); }
void FieldVisitorReadBinary::read(Int128 & value, ReadBuffer & buf) { readBinary(value, buf); }
void FieldVisitorReadBinary::read(Int256 & value, ReadBuffer & buf) { readBinary(value, buf); }
void FieldVisitorReadBinary::read(UUID & value, ReadBuffer & buf) { readBinary(value, buf); }
void FieldVisitorReadBinary::read(IPv4 & value, ReadBuffer & buf) { readBinary(value, buf); }
void FieldVisitorReadBinary::read(IPv6 & value, ReadBuffer & buf) { readBinary(value, buf); }
void FieldVisitorReadBinary::read(Float64 & value, ReadBuffer & buf) { readBinary(value, buf); }
void FieldVisitorReadBinary::read(String & value, ReadBuffer & buf) { readBinary(value, buf); }

void FieldVisitorReadBinary::read(DecimalField<Decimal32> & value, ReadBuffer & buf)
{
    Decimal32 data;
    UInt32 scale;
    readBinary(data, buf);
    readBinary(scale, buf);
    value = DecimalField<Decimal32>(data, scale);
}
    
void FieldVisitorReadBinary::read(DecimalField<Decimal64> & value, ReadBuffer & buf)
{
    Decimal64 data;
    UInt32 scale;
    readBinary(data, buf);
    readBinary(scale, buf);

    value = DecimalField<Decimal64>(data, scale);
}
    
void FieldVisitorReadBinary::read(DecimalField<Decimal128> & value, ReadBuffer & buf)
{
    Decimal128 data;
    UInt32 scale;
    readBinary(data, buf);
    readBinary(scale, buf);
    value = DecimalField<Decimal128>(data, scale);
}
    
void FieldVisitorReadBinary::read(DecimalField<Decimal256> & value, ReadBuffer & buf)
{
    Decimal256 data;
    UInt32 scale;
    readBinary(data, buf);
    readBinary(scale, buf);
    value = DecimalField<Decimal256>(data, scale);
}

void FieldVisitorReadBinary::read(AggregateFunctionStateData & value, ReadBuffer & buf)
{
    readStringBinary(value.name, buf);
    readStringBinary(value.data, buf);
}

void FieldVisitorReadBinary::read(Array & value, ReadBuffer & buf)
{
    size_t size{0};
    readBinary(size, buf);

    for (size_t i = 0; i < size; ++i)
    {
        UInt8 type{0};
        readBinary(type, buf);
        auto tp = static_cast<Field::Types::Which>(type);
        auto f = dispatchField(FieldVisitorReadBinary(buf), tp);
        value.push_back(f);
    }
}

void FieldVisitorReadBinary::read(Tuple & value, ReadBuffer & buf)
{
    size_t size{0};
    readBinary(size, buf);

    for (size_t i = 0; i < size; ++i)
    {
        UInt8 type{0};
        readBinary(type, buf);
        auto f = dispatchField(FieldVisitorReadBinary(buf), static_cast<Field::Types::Which>(type));
        value.push_back(f);
    }
}

/// Match FieldVisitorWriteBinary::operator() (const Map & x, WriteBuffer & buf)
/// The Map type of the community is FieldVector
void FieldVisitorReadBinary::read(Map & value, ReadBuffer & buf)
{
    size_t size{0};
    readBinary(size, buf);

    for (size_t i = 0; i < size; ++i)
    {
        UInt8 type{0};
        readBinary(type, buf);
        auto value_field = dispatchField(FieldVisitorReadBinary(buf), static_cast<Field::Types::Which>(type));
        value.push_back(value_field);
    }
}

void FieldVisitorReadBinary::read(Object & value, ReadBuffer & buf)
{
    size_t size;
    readBinary(size, buf);

    for (size_t index = 0; index < size; ++index)
    {
        UInt8 type;
        String key;
        readBinary(type, buf);
        readBinary(key, buf);
        value[key] = dispatchField(FieldVisitorReadBinary(buf), static_cast<Field::Types::Which>(type));
    }
}

void FieldVisitorReadBinary::read(bool & x, ReadBuffer & buf)
{
    readBinary(x, buf);
}

}
