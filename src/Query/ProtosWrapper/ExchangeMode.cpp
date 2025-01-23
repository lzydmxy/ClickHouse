#include "ExchangeMode.h"
#include <sstream>

namespace DB
{

String exchangeModeToString(const RExchangeMode::Enum & exchange_mode)
{
    if (exchange_mode == RExchangeMode::UNKNOWN)
        return "UNKNOWN";
    else if (exchange_mode == RExchangeMode::LOCAL_NO_NEED_REPARTITION)
        return "LOCAL_NO_NEED_REPARTITION";
    else if (exchange_mode == RExchangeMode::LOCAL_MAY_NEED_REPARTITION)
        return "LOCAL_MAY_NEED_REPARTITION";
    else if (exchange_mode == RExchangeMode::REPARTITION)
        return "REPARTITION";
    else if (exchange_mode == RExchangeMode::BROADCAST)
        return "BROADCAST";
    else if (exchange_mode == RExchangeMode::GATHER)
        return "GATHER";
    else if (exchange_mode == RExchangeMode::BUCKET_REPARTITION)
        return "BUCKET_REPARTITION";
    else
        return "";
}

bool isLocalExchange(const RExchangeMode::Enum & mode)
{
    return mode == RExchangeMode::LOCAL_NO_NEED_REPARTITION || mode == RExchangeMode::LOCAL_MAY_NEED_REPARTITION;
}

}
