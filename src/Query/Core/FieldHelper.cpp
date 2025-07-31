#include "FieldHelper.h"


namespace DB
{

// used for both protobuf and original serde
void FieldHelper::writeFieldBinaryBlobImpl(const Field & field, Field::Types::Which type, WriteBuffer & buf)
{
    switch (type)
    {
        case Field::Types::Null:
        {
            return;
        }
        case Field::Types::UInt64:
        {
            writeBinary(field.get<UInt64>(), buf);
            return;
        }
        case Field::Types::Int64:
        {
            writeBinary(field.get<Int64>(), buf);
            return;
        }
        case Field::Types::Float64:
        {
            writeBinary(field.get<Float64>(), buf);
            return;
        }
        case Field::Types::UInt128:
        {
            writeBinary(field.get<UInt128>(), buf);
            return;
        }
        case Field::Types::Int128:
        {
            writeBinary(field.get<Int128>(), buf);
            return;
        }
        case Field::Types::UInt256:
        {
            writeBinary(field.get<UInt256>(), buf);
            return;
        }
        case Field::Types::Int256:
        {
            writeBinary(field.get<Int256>(), buf);
            return;
        }
        case Field::Types::String:
        {
            writeBinary(field.get<String>(), buf);
            return;
        }
        case Field::Types::Array:
        {
            writeBinary(field.get<Array>(), buf);
            return;
        }
        case Field::Types::Tuple:
        {
            writeBinary(field.get<Tuple>(), buf);
            return;
        }
        case Field::Types::IPv4:
        {
            writeBinary(field.get<IPv4>(), buf);
            return;
        }
        case Field::Types::IPv6:
        {
            writeBinary(field.get<IPv6>(), buf);
            return;
        }
        case Field::Types::Decimal32:
        {
            auto df = field.get<DecimalField<Decimal32>>();
            writeBinary(df.getValue(), buf);
            writeBinary(df.getScale(), buf);
            return;
        }
        case Field::Types::Decimal64:
        {
            auto df = field.get<DecimalField<Decimal64>>();
            writeBinary(df.getValue(), buf);
            writeBinary(df.getScale(), buf);
            return;
        }
        case Field::Types::Decimal128:
        {
            auto df = field.get<DecimalField<Decimal128>>();
            writeBinary(df.getValue(), buf);
            writeBinary(df.getScale(), buf);
            return;
        }
        case Field::Types::Decimal256:
        {
            auto df = field.get<DecimalField<Decimal256>>();
            writeBinary(df.getValue(), buf);
            writeBinary(df.getScale(), buf);
            return;
        }
        case Field::Types::Map:
        {
            auto df = field.get<Map>();
            writeBinary(df, buf);
            return;
        }
        case Field::Types::UUID:
        {
            writeBinary(field.get<UUID>(), buf);
            return;
        }
        case Field::Types::AggregateFunctionState:
        {
            writeStringBinary(field.get<AggregateFunctionStateData>().name, buf);
            writeStringBinary(field.get<AggregateFunctionStateData>().data, buf);
            return;
        }
        case Field::Types::Bool:
        {
            writeBinary(field.get<bool>(), buf);
            return;
        }
        case Field::Types::Object:
        {
            writeBinary(field.get<Object>(), buf);
            return;
        }
        case Field::Types::CustomType:
        {
            writeBinary(field.get<CustomType>(), buf);
            return;
        }
        case Field::Types::SketchBinary:
        {
            writeBinary(field.get<String>(), buf);
            return;
        }
        // default:
        //     throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Bad type of Field {} when serializing.", type);
    }
}

// used for both protobuf and original serde
void FieldHelper::readFieldBinaryBlobImpl(Field & field, Field::Types::Which type, ReadBuffer & buf)
{
    switch (type)
    {
        case Field::Types::Null:
        {
            field = Field();
            return;
        }
        case Field::Types::UInt64:
        {
            UInt64 value;
            readBinary(value, buf);
            field = value;
            return;
        }
        case Field::Types::Int64:
        {
            Int64 value;
            readBinary(value, buf);
            field = value;
            return;
        }
        case Field::Types::Float64:
        {
            Float64 value;
            readBinary(value, buf);
            field = value;
            return;
        }
        case Field::Types::UInt128:
        {
            UInt128 value;
            readBinary(value, buf);
            field = value;
            return;
        }
        case Field::Types::Int128:
        {
            Int128 value;
            readBinary(value, buf);
            field = value;
            return;
        }
        case Field::Types::UInt256:
        {
            UInt256 value;
            readBinary(value, buf);
            field = value;
            return;
        }
        case Field::Types::Int256:
        {
            Int256 value;
            readBinary(value, buf);
            field = value;
            return;
        }
        case Field::Types::String:
        {
            String value;
            readBinary(value, buf);
            field = value;
            return;
        }
        case Field::Types::Array:
        {
            Array value;
            readBinary(value, buf);
            field = value;
            return;
        }
        case Field::Types::Tuple:
        {
            Tuple value;
            readBinary(value, buf);
            field = value;
            return;
        }
        case Field::Types::Decimal32:
        {
            Decimal32 value;
            UInt32 scale;
            readBinary(value, buf);
            readBinary(scale, buf);
            field = DecimalField<Decimal32>(value, scale);
            return;
        }
        case Field::Types::Decimal64:
        {
            Decimal64 value;
            UInt32 scale;
            readBinary(value, buf);
            readBinary(scale, buf);
            field = DecimalField<Decimal64>(value, scale);
            return;
        }
        case Field::Types::Decimal128:
        {
            Decimal128 value;
            UInt32 scale;
            readBinary(value, buf);
            readBinary(scale, buf);
            field = DecimalField<Decimal128>(value, scale);
            return;
        }
        case Field::Types::Decimal256:
        {
            Decimal256 value;
            UInt32 scale;
            readBinary(value, buf);
            readBinary(scale, buf);
            field = DecimalField<Decimal256>(value, scale);
            return;
        }
        case Field::Types::Map:
        {
            Map value;
            readBinary(value, buf);
            field = value;
            return;
        }
        case Field::Types::UUID:
        {
            UUID value;
            readBinary(value, buf);
            field = value;
            return;
        }
        case Field::Types::AggregateFunctionState:
        {
            AggregateFunctionStateData value;
            readStringBinary(value.name, buf);
            readStringBinary(value.data, buf);
            field = value;
            return;
        }
        case Field::Types::Bool:
        {
            UInt64 value;
            readBinary(value, buf);
            field = static_cast<bool>(value);
            return;
        }
        case Field::Types::Object:
        {
            Object value;
            readBinary(value, buf);
            field = value;
            return;
        }
        default:
            throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Bad type of Field {} when deserializing.", type);
    }
}
}
