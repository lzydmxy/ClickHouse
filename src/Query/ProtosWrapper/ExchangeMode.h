#pragma once

#include <Core/Types.h>
#include <Query/ProtosWrapper/QueryProto.h>

namespace DB
{

String exchangeModeToString(const RExchangeMode::Enum & exchange_mode);
bool isLocalExchange(const RExchangeMode::Enum & mode);

}
