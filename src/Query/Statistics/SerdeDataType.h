#pragma once

#include <Query/Statistics/TypeMacros.h>

#include <Query/Protos/optimizer_statistics.pb.h>

namespace DB::QueryStatistics
{
    using SerdeDataType = Protos::SerdeDataType;
    static_assert(sizeof(SerdeDataType) == 4);

    template <typename T>
    inline constexpr SerdeDataType SerdeDataTypeFrom = SerdeDataType::Nothing;

#define CASE(TYPE) \
    template <> \
    inline constexpr SerdeDataType SerdeDataTypeFrom<TYPE> = SerdeDataType::TYPE;
ALL_TYPE_ITERATE(CASE)
#undef CASE

}
