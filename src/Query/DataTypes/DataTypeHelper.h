#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>

namespace DB
{

/// IDataType helpers (alternative for IDataType virtual methods with single point of truth)

template <typename T>
inline bool isDate(const T & data_type) { return WhichDataType(data_type).isDate(); }
template <typename T>
inline bool isDate32(const T & data_type) { return WhichDataType(data_type).isDate32(); }
template <typename T>
inline bool isDateOrDate32(const T & data_type) { return WhichDataType(data_type).isDateOrDate32(); }
template <typename T>
inline bool isDateOrDateTime(const T & data_type) { return WhichDataType(data_type).isDateOrDate32() || WhichDataType(data_type).isDateTimeOrDateTime64(); }
// template <typename T>
// diff byconity has data type Time
// inline bool isTime(const T & data_type) { return WhichDataType(data_type).isTime(); }
template <typename T>
inline bool isDateTime(const T & data_type) { return WhichDataType(data_type).isDateTime(); }
template <typename T>
inline bool isDateTime64(const T & data_type) { return WhichDataType(data_type).isDateTime64(); }

inline bool isEnum8(const DataTypePtr & data_type) { return WhichDataType(data_type).isEnum8(); }
inline bool isEnum16(const DataTypePtr & data_type) { return WhichDataType(data_type).isEnum16(); }
inline bool isEnum(const DataTypePtr & data_type) { return WhichDataType(data_type).isEnum(); }
inline bool isDecimal(const DataTypePtr & data_type) { return WhichDataType(data_type).isDecimal(); }
inline bool isTuple(const DataTypePtr & data_type) { return WhichDataType(data_type).isTuple(); }
inline bool isArray(const DataTypePtr & data_type) { return WhichDataType(data_type).isArray(); }
inline bool isMap(const DataTypePtr & data_type) { return WhichDataType(data_type).isMap(); }
inline bool isInterval(const DataTypePtr & data_type) {return WhichDataType(data_type).isInterval(); }
inline bool isNothing(const DataTypePtr & data_type) { return WhichDataType(data_type).isNothing(); }
inline bool isUUID(const DataTypePtr & data_type) { return WhichDataType(data_type).isUUID(); }
inline bool isIPv4(const DataTypePtr & data_type) { return WhichDataType(data_type).isIPv4(); }
inline bool isIPv6(const DataTypePtr & data_type) { return WhichDataType(data_type).isIPv6(); }
// diff byconity has data type JsonB and Bitmap64
// inline bool isBitmap64(const DataTypePtr & data_type) { return WhichDataType(data_type).isBitmap64(); }
// inline bool isJsonb(const DataTypePtr & data_type) { return WhichDataType(data_type).isJsonb(); }

template <typename T>
inline bool isObject(const T & data_type)
{
    return WhichDataType(data_type).isObject();
}

template <typename T>
inline bool isUInt8(const T & data_type)
{
    return WhichDataType(data_type).isUInt8();
}

template <typename T>
inline bool isUnsignedInteger(const T & data_type)
{
    return WhichDataType(data_type).isUInt();
}

template <typename T>
inline bool isSignedInteger(const T & data_type)
{
    return WhichDataType(data_type).isInt();
}

template <typename T>
inline bool isInteger(const T & data_type)
{
    WhichDataType which(data_type);
    return which.isInt() || which.isUInt();
}

template <typename T>
inline bool isFloat(const T & data_type)
{
    WhichDataType which(data_type);
    return which.isFloat();
}

template <typename T>
inline bool isNativeUInt(const T & data_type)
{
    WhichDataType which(data_type);
    return which.isNativeUInt();
}

template <typename T>
inline bool isNativeInteger(const T & data_type)
{
    WhichDataType which(data_type);
    return which.isNativeInt() || which.isNativeUInt();
}


template <typename T>
inline bool isNativeNumber(const T & data_type)
{
    WhichDataType which(data_type);
    return which.isNativeInt() || which.isNativeUInt() || which.isFloat();
}

template <typename T>
inline bool isNumber(const T & data_type)
{
    WhichDataType which(data_type);
    return which.isInt() || which.isUInt() || which.isFloat() || which.isDecimal();
}

template <typename T>
inline bool isColumnedAsDecimal(const T & data_type)
{
    WhichDataType which(data_type);
    return which.isDecimal() || which.isDateTime64();
}

// Same as isColumnedAsDecimal but also checks value type of underlyig column.

template <typename T>
inline bool isString(const T & data_type)
{
    return WhichDataType(data_type).isString();
}

template <typename T>
inline bool isFixedString(const T & data_type)
{
    return WhichDataType(data_type).isFixedString();
}

template <typename T>
inline bool isStringOrFixedString(const T & data_type)
{
    return WhichDataType(data_type).isStringOrFixedString();
}

template <typename T>
inline bool isNumberOrString(const T & data_type)
{
    WhichDataType which(data_type);
    return which.isNumber() || which.isStringOrFixedString();
}

template <typename T>
inline bool isNotCreatable(const T & data_type)
{
    WhichDataType which(data_type);
    return which.isNothing() || which.isFunction() || which.isSet();
}

inline bool isNotDecimalButComparableToDecimal(const DataTypePtr & data_type)
{
    WhichDataType which(data_type);
    return which.isInt() || which.isUInt() || which.isFloat();
}

inline bool isCompilableType(const DataTypePtr & data_type)
{
    return data_type->isValueRepresentedByNumber() && !isDecimal(data_type);
}

struct DataTypeRange
{
    Field min;
    Field max;
};

template <typename T>
inline std::optional<DataTypeRange> getRangeForNumeric()
{
    return DataTypeRange{std::numeric_limits<T>::min(), std::numeric_limits<T>::max()};
}

// Float32 doesn't have a well-defined range, because of Inf, -Inf, NaN.
template <>
inline std::optional<DataTypeRange> getRangeForNumeric<Float32>()
{
    return std::nullopt;
}

// Float64 doesn't have a well-defined range, because of Inf, -Inf, NaN.
template <>
inline std::optional<DataTypeRange> getRangeForNumeric<Float64>()
{
    return std::nullopt;
}

inline std::optional<DataTypeRange> getRangeFromDataType(const DataTypePtr & data_type)
{
    if (dynamic_cast<const DataTypeUInt8 *>(data_type.get()))
        return getRangeForNumeric<UInt8>();
    if (dynamic_cast<const DataTypeUInt16 *>(data_type.get()))
        return getRangeForNumeric<UInt16>();
    if (dynamic_cast<const DataTypeUInt32 *>(data_type.get()))
        return getRangeForNumeric<UInt32>();
    if (dynamic_cast<const DataTypeUInt64 *>(data_type.get()))
        return getRangeForNumeric<UInt64>();
    if (dynamic_cast<const DataTypeInt8 *>(data_type.get()))
        return getRangeForNumeric<Int8>();
    if (dynamic_cast<const DataTypeInt16 *>(data_type.get()))
        return getRangeForNumeric<Int16>();
    if (dynamic_cast<const DataTypeInt32 *>(data_type.get()))
        return getRangeForNumeric<Int32>();
    if (dynamic_cast<const DataTypeInt64 *>(data_type.get()))
        return getRangeForNumeric<Int64>();
    if (dynamic_cast<const DataTypeFloat32 *>(data_type.get()))
        return getRangeForNumeric<Float32>();
    if (dynamic_cast<const DataTypeFloat64 *>(data_type.get()))
        return getRangeForNumeric<Float64>();
    if (dynamic_cast<const DataTypeUInt128 *>(data_type.get()))
        return getRangeForNumeric<UInt128>();
    if (dynamic_cast<const DataTypeInt128 *>(data_type.get()))
        return getRangeForNumeric<Int128>();
    if (dynamic_cast<const DataTypeUInt256 *>(data_type.get()))
        return getRangeForNumeric<UInt256>();
    if (dynamic_cast<const DataTypeInt256 *>(data_type.get()))
        return getRangeForNumeric<Int256>();
    return std::nullopt;
}

}
