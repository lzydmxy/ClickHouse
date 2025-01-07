#include "ExchangeMode.h"
#include <sstream>

namespace DB
{

String exchangeModeToString(const RPCExchangeMode & exchange_mode)
{
    std::ostringstream ostr;
    switch(exchange_mode)
    {
        case RPCExchangeMode::UNKNOWN:
            ostr << "UNKNOWN";
            break;
        case RPCExchangeMode::LOCAL_NO_NEED_REPARTITION:
            ostr << "LOCAL_NO_NEED_REPARTITION";
            break;
        case RPCExchangeMode::LOCAL_MAY_NEED_REPARTITION:
            ostr << "LOCAL_MAY_NEED_REPARTITION";
            break;
        case RPCExchangeMode::REPARTITION:
            ostr << "REPARTITION";
            break;
        case RPCExchangeMode::BROADCAST:
            ostr << "BROADCAST";
            break;
        case RPCExchangeMode::GATHER:
            ostr << "GATHER";
            break;
        case RPCExchangeMode::BUCKET_REPARTITION:
            ostr << "BUCKET_REPARTITION";
            break;
    }

    return ostr.str();
}

bool isLocalExchange(RPCExchangeMode mode)
{
    return mode == RPCExchangeMode::LOCAL_NO_NEED_REPARTITION || mode == RPCExchangeMode::LOCAL_MAY_NEED_REPARTITION;
}

}
