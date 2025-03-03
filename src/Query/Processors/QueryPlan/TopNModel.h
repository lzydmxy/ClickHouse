#pragma once

#include <Query/Protos/EnumMacros.h>
#include <Query/Protos/common.pb.h>

namespace DB
{

ENUM_WITH_PROTO_CONVERTER(
    TopNModel, // enum name
    Protos::TopNModel, // proto enum message
    (ROW_NUMBER, 0),
    (RANKER, 1),
    (DENSE_RANK, 2));
}
