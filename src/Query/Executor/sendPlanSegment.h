#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Query/ProtosHelper/AddressInfo.h>
#include <Query/ProtosHelper/HostWithPorts.h>
#include <Query/Executor/DAGGraph.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/Executor/PlanSegmentInstance.h>
#include <Query/Executor/executePlanSegment.h>

namespace DB
{

void sendPlanSegmentToAddress(
    const AddressInfo & address_info,
    PlanSegment * plan_segment_ptr,
    PlanSegmentExecutionInfo & execution_info,
    ContextPtr query_context,
    std::shared_ptr<DAGGraph> dag_graph_ptr,
    std::shared_ptr<butil::IOBuf> plan_segment_buf_ptr,
    const WorkerID & worker_id);

void sendPlanSegmentsToAddress(
    const AddressInfo & address_info,
    const PlanSegmentHeaders & plan_segment_headers,
    ContextPtr query_context,
    std::shared_ptr<DAGGraph> dag_graph_ptr,
    const WorkerID & worker_id);

using SendPlanSegmentToAddressFunc = std::function<void(
    const AddressInfo & address_info,
    PlanSegment * plan_segment_ptr,
    PlanSegmentExecutionInfo & execution_info,
    ContextPtr query_context,
    std::shared_ptr<DAGGraph> dag_graph_ptr,
    std::shared_ptr<butil::IOBuf> plan_segment_buf_ptr,
    const WorkerID & worker_id)>;
}
