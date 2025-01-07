#pragma once

#include <Core/Types.h>
#include <Query/ProtosWrapper/QueryProto.h>

namespace DB
{

String exchangeModeToString(const RPCExchangeMode & exchange_mode);
bool isLocalExchange(RPCExchangeMode mode);

}
