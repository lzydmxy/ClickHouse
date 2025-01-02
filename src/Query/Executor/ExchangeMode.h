#pragma once

#include <Core/Types.h>
#include <Query/Protos/EnumMacros.h>
#include <Query/Protos/enum.pb.h>

namespace DB
{

ENUM_WITH_PROTO_CONVERTER(
    ExchangeMode, // enum name
    Protos::ExchangeMode, // proto enum message
    (UNKNOWN, 0),
    (LOCAL_NO_NEED_REPARTITION, 1), /// for global join, if we want to increase the parallel size, just split it
    (LOCAL_MAY_NEED_REPARTITION, 2), /// for local join, if we want to increase the parallel size, we need repartition
    (REPARTITION, 3),
    (BROADCAST, 4),
    (GATHER, 5),
    (BUCKET_REPARTITION, 6));

String exchangeModeToString(const ExchangeMode & exchange_mode);
bool isLocalExchange(ExchangeMode mode);
}
