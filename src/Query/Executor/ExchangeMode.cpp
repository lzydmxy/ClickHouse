#include <Query/Executor/ExchangeMode.h>
#include <sstream>

namespace DB
{

String exchangeModeToString(const ExchangeMode & exchange_mode)
{
    std::ostringstream ostr;

    switch(exchange_mode)
    {
        case ExchangeMode::UNKNOWN:
            ostr << "UNKNOWN";
            break;
        case ExchangeMode::LOCAL_NO_NEED_REPARTITION:
            ostr << "LOCAL_NO_NEED_REPARTITION";
            break;
        case ExchangeMode::LOCAL_MAY_NEED_REPARTITION:
            ostr << "LOCAL_MAY_NEED_REPARTITION";
            break;
        case ExchangeMode::REPARTITION:
            ostr << "REPARTITION";
            break;
        case ExchangeMode::BROADCAST:
            ostr << "BROADCAST";
            break;
        case ExchangeMode::GATHER:
            ostr << "GATHER";
            break;
        case ExchangeMode::BUCKET_REPARTITION:
            ostr << "BUCKET_REPARTITION";
            break;
    }

    return ostr.str();
}

bool isLocalExchange(ExchangeMode mode)
{
    return mode == ExchangeMode::LOCAL_NO_NEED_REPARTITION || mode == ExchangeMode::LOCAL_MAY_NEED_REPARTITION;
}
}
