#include "QueryProto.h"

namespace DB
{

String planSegmentTypeToString(const RIPlanSegment::Enum & type)
{
    std::ostringstream ostr;
    if(type == RIPlanSegment::UNKNOWN)
        ostr << "UNKNOWN";
    else if (type == RIPlanSegment::SOURCE)
        ostr << "SOURCE";
    else if (type == RIPlanSegment::EXCHANGE)
        ostr << "EXCHANGE";
    else if (type == RIPlanSegment::OUTPUT)
        ostr << "OUTPUT";
    return ostr.str();
}

}
