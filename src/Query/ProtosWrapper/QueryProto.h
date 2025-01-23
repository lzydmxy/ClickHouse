
#include <Core/Types.h>
#include <Query/Protos/common.pb.h>
#include <Query/Protos/plan_node.pb.h>
#include <Query/Protos/execute_plan_service.pb.h>

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

using RSourceTaskFilter = Protos::SourceTaskFilter;

String planSegmentTypeToString(const RIPlanSegment::Enum & type);

}
