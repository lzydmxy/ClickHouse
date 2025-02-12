
#include <Core/Types.h>
#include <Query/Protos/common.pb.h>
#include <Query/Protos/plan_node.pb.h>
#include <Query/Protos/plan_segment_service.pb.h>

namespace DB
{

using RUUID = Protos::UUID;
using RStorageID = Protos::StorageID;

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

using RHostWithPorts = Protos::HostWithPorts;
using RInputProfileMetric = Protos::InputProfileMetric;
using RProfileMetric = Protos::ProfileMetric;

using RPlanSegmentBody = Protos::PlanSegmentBody;
using RRuntimeFilter = Protos::RuntimeFilter;

using RQueryCommon = Protos::QueryCommon;
using RQueryCommonPtr = std::shared_ptr<RQueryCommon>;
using RPlanSegmentRequest = Protos::PlanSegmentRequest;
using RPlanSegmentResponse = Protos::PlanSegmentResponse;
using RPlanSegmentsRequest = Protos::PlanSegmentsRequest;
using RPlanSegmentsResponse = Protos::PlanSegmentsResponse;
using RCancelQueryRequest = Protos::CancelQueryRequest;
using RCancelQueryResponse = Protos::CancelQueryResponse;
using RPlanSegmentStatusRequest = Protos::PlanSegmentStatusRequest;
using RPlanSegmentStatusResponse = Protos::PlanSegmentStatusResponse;
using RPlanSegmentProfileRequest = Protos::PlanSegmentProfileRequest;
using RPlanSegmentProfileResponse = Protos::PlanSegmentProfileResponse;

//For bRPC server
using RPlanSegmentService = Protos::PlanSegmentService;
//For bRPC client
using RPlanSegmentServiceStub = Protos::PlanSegmentService_Stub;

String planSegmentTypeToString(const RIPlanSegment::Enum & type);

}
