
#include <Core/Types.h>
#include "common.pb.h"
#include "plan_node.pb.h"
#include "plan_segment.pb.h"

namespace DB
{

using RExchangeMode  = Protos::ExchangeMode;
using RReportProfileType = Protos::ReportProfileType;

using RAddressInfo = Protos::AddressInfo;
using RPlanSegmentPartitionSource = Protos::PlanSegmentPartitionSource;
using RSourceTaskFilter = Protos::SourceTaskFilter;

using RIPlanSegment = Protos::IPlanSegment;
using RPlanSegmentInput = Protos::PlanSegmentInput;
using RPlanSegmentOutput = Protos::PlanSegmentOutput;
using RPlanSegment = Protos::PlanSegment;

String planSegmentTypeToString(const RIPlanSegment::Enum & type);

}
