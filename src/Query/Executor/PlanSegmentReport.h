#pragma once
#include <Common/logger_useful.h>
#include <Interpreters/Context_fwd.h>
#include <Query/ProtosHelper/QueryProto.h>
#include <Query/ProtosHelper/AddressInfo.h>
#include <Query/Executor/PlanSegmentExecutor.h>
#include <Query/Executor/PlanSegment.h>

namespace DB
{

void reportExecutionResult(const PlanSegmentExecutor::ExecutionResult & result, bool inform_success_status = false) noexcept;

PlanSegmentExecutor::ExecutionResult convertFailurePlanSegmentStatusToResult(
    ContextPtr query_context,
    const PlanSegmentExecutionInfo & execution_info,
    int exception_code,
    const String & exception_message,
    Progress final_progress = {},
    SenderMetrics sender_metrics = {},
    PlanSegmentOutputs plan_segment_outputs = {});


PlanSegmentExecutor::ExecutionResult convertSuccessPlanSegmentStatusToResult(
    ContextPtr query_context,
    const PlanSegmentExecutionInfo & execution_info,
    Progress & final_progress,
    SenderMetrics & sender_metrics,
    PlanSegmentOutputs & plan_segment_outputs,
    PlanSegmentProfilePtr & segment_profile);

}
