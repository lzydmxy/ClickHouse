#include "ExchangeMode.h"
#include <sstream>

namespace DB
{

String exchangeModeToString(const RExchangeMode & exchange_mode)
{
    std::ostringstream ostr;
    switch(exchange_mode)
    {
        case RExchangeMode::UNKNOWN:
            ostr << "UNKNOWN";
            break;
        case RExchangeMode::LOCAL_NO_NEED_REPARTITION:
            ostr << "LOCAL_NO_NEED_REPARTITION";
            break;
        case RExchangeMode::LOCAL_MAY_NEED_REPARTITION:
            ostr << "LOCAL_MAY_NEED_REPARTITION";
            break;
        case RExchangeMode::REPARTITION:
            ostr << "REPARTITION";
            break;
        case RExchangeMode::BROADCAST:
            ostr << "BROADCAST";
            break;
        case RExchangeMode::GATHER:
            ostr << "GATHER";
            break;
        case RExchangeMode::BUCKET_REPARTITION:
            ostr << "BUCKET_REPARTITION";
            break;
    }

    return ostr.str();
}

bool isLocalExchange(RExchangeMode mode)
{
    return mode == RExchangeMode::LOCAL_NO_NEED_REPARTITION || mode == RExchangeMode::LOCAL_MAY_NEED_REPARTITION;
}

}
