
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
using RRuntimeFilter = Protos::RuntimeFilter;

using RSendBytesByParallelIndex = Protos::SendBytesByParallelIndex;
using RRuntimeSegmentsMetrics = Protos::RuntimeSegmentsMetrics;
using RSenderMetrics = Protos::SenderMetrics;

using RQueryCommon = Protos::QueryCommon;
using RQueryCommonPtr = std::shared_ptr<RQueryCommon>;

using RPlanSegmentRequest = Protos::PlanSegmentRequest;
using RPlanSegmentHeader = Protos::PlanSegmentHeader;
using RPlanSegmentsRequest = Protos::PlanSegmentsRequest;
using RPlanSegmentResponse = Protos::PlanSegmentResponse;

using RCancelQueryRequest = Protos::CancelQueryRequest;
using RCancelQueryResponse = Protos::CancelQueryResponse;

using RPlanSegmentStatusRequest = Protos::PlanSegmentStatusRequest;
using RPlanSegmentStatusResponse = Protos::PlanSegmentStatusResponse;

using RPlanSegmentProfileRequest = Protos::PlanSegmentProfileRequest;
using RPlanSegmentProfileResponse = Protos::PlanSegmentProfileResponse;

using RAST = Protos::AST;
using RDataType = Protos::DataType;
using RBlock = Protos::Block;
using RAggregateFunction = Protos::AggregateFunction;
using RQueryPlanStep = Protos::QueryPlanStep;
using RFieldVector = Protos::FieldVector;

using RProgress = Protos::Progress;

using RQueryPlan = Protos::QueryPlan;

//For bRPC server
using RPlanSegmentService = Protos::PlanSegmentService;
//For bRPC client
using RPlanSegmentServiceStub = Protos::PlanSegmentService_Stub;

using RProgressRequest = Protos::ProgressRequest;
using RProgressResponse = Protos::ProgressResponse;
using RProcessorProfileRequest = Protos::ProcessorProfileRequest;
using RProcessorsProfileRequest = Protos::ProcessorsProfileRequest;
using RProcessorProfileResponse = Protos::ProcessorProfileResponse;


String planSegmentTypeToString(const RIPlanSegment::Enum & type);

}
