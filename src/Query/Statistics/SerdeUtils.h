#pragma once

#include <DataTypes/IDataType.h>
#include <Query/Statistics/SerdeDataType.h>
#include <fmt/format.h>

#include <string_view>
#include <tuple>

namespace DB::QueryStatistics
{
template <typename HeaderType = SerdeDataType>
inline std::tuple<HeaderType, std::string_view> parseBlobWithHeader(std::string_view raw_blob)
{
    static_assert(std::is_trivial_v<HeaderType>);
    if (raw_blob.size() <= sizeof(HeaderType))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "corrupted blob");
    }

    HeaderType header;
    memcpy(&header, raw_blob.data(), sizeof(header));
    auto blob = raw_blob.substr(sizeof(header), raw_blob.size() - sizeof(header));
    return {header, blob};
}

template <typename T>
constexpr bool IsWideInteger = wide::IsWideInteger<T>::value;
template <typename T>
void checkSerdeDataType(SerdeDataType serde_data_type)
{
    if (std::is_same_v<T, String> && serde_data_type == SerdeDataType::StringOldVersion)
        return;

    if (serde_data_type != SerdeDataTypeFrom<T>)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "mismatched type");
    }
}

inline void assertOk(bool ok, const char * expr)
{
    if (!ok)
    {
        auto err_msg = fmt::format(FMT_STRING("parse from blob failed when executing ({})"), expr);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "failed to parse from blob");
    }
}

#define ASSERT_PARSE(EXPR) assertOk((EXPR), #EXPR)

}
