#pragma once

#include <Core/Types.h>
#include <Query/ProtosHelper/QueryProto.h>

namespace DB
{

RExchangeMode::Enum toExchangeMode(int mode);
String exchangeModeToString(const RExchangeMode::Enum & exchange_mode);
bool isLocalExchange(const RExchangeMode::Enum & mode);

}
