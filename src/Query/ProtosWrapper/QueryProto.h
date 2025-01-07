
#include <Core/Types.h>
#include "common.pb.h"
#include "plan_node.pb.h"

namespace DB
{
    using RPCExchangeMode  = DB::Protos::ExchangeMode;
    using RPCAddressInfo = DB::Protos::AddressInfo;
    using RPCPlanSegmentPartitionSource = DB::Protos::PlanSegmentPartitionSource;
    using RPCSourceTaskFilter = DB::Protos::SourceTaskFilter;
}
