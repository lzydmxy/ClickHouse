#include "QueryProto.h"

String planSegmentTypeToString(const RIPlanSegment::Enum & type)
{
    std::ostringstream ostr;
    switch(type)
    {
        case PlanSegmentType::UNKNOWN:
            ostr << "UNKNOWN";
            break;
        case PlanSegmentType::SOURCE:
            ostr << "SOURCE";
            break;
        case PlanSegmentType::EXCHANGE:
            ostr << "EXCHANGE";
            break;
        case PlanSegmentType::OUTPUT:
            ostr << "OUTPUT";
            break;
    }
    return ostr.str();
}
