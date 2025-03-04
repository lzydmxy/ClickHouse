#include "DAGGraph.h"
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BRPC_EXCEPTION;
}

void DAGGraph::joinAsyncRpcPerStage()
{
    if (optimizer_context->getSettingsRef().send_plan_segment_by_brpc_join_at_last)
        return;
    if (optimizer_context->getSettingsRef().send_plan_segment_by_brpc_join_per_stage)
        joinAsyncRpcWithThrow();
}

void DAGGraph::joinAsyncRpcWithThrow()
{
    auto async_ret = async_context->wait();
    if (async_ret.status == AsyncContext::AsyncStats::FAILED)
        throw Exception(ErrorCodes::BRPC_EXCEPTION, "send plan segment async failed error code : {} error worker : {} error text : {}"
            ,async_ret.error_code, async_ret.failed_worker, async_ret.error_text);
}

void DAGGraph::joinAsyncRpcAtLast()
{
    if (optimizer_context->getSettingsRef().send_plan_segment_by_brpc_join_at_last)
        joinAsyncRpcWithThrow();
}

/// return addresses order by parallel id
AddressInfos DAGGraph::getAddressInfos(size_t segment_id)
{
    /// for bsp_mode we need get worker addresses from finished_address, because retry might happen
    if (!id_to_address.contains(segment_id))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Logical error: address of segment {} can not be found in id_to_address", segment_id);
    }
    return id_to_address[segment_id];
}

void SourcePruner::generateSegmentStorageMap()
{
    for (auto & node : plan_segments_ptr->getNodes())
    {
        for (auto & segment_input : node.getPlanSegment()->getPlanSegmentInputs())
        {
            if (segment_input->getStorageID())
            {
                auto uuid = segment_input->getStorageID()->uuid;
                LOG_TRACE(
                    log,
                    "SourcePrune plan segment {} storage id : {}",
                    node.getPlanSegment()->getPlanSegmentId(),
                    segment_input->getStorageID()->getNameForLogs());
                if (segment_input->getStorageID()->hasUUID())
                {
                    plan_segment_storages_map[node.getPlanSegment()->getPlanSegmentId()].insert(uuid);
                }
            }
        }
    }
}

void SourcePruner::generateUnprunableSegments()
{
    for (auto & node : plan_segments_ptr->getNodes())
    {
        for (auto & segment_output : node.getPlanSegment()->getPlanSegmentOutputs())
        {
            if (segment_output->getExchangeMode() == RExchangeMode::LOCAL_MAY_NEED_REPARTITION
                || segment_output->getExchangeMode() == RExchangeMode::LOCAL_NO_NEED_REPARTITION)
            {
                unprunable_plan_segments.insert(node.getPlanSegment()->getPlanSegmentId());
                unprunable_plan_segments.insert(segment_output->getPlanSegmentId());
            }
        }
        for (const auto & segment_input : node.getPlanSegment()->getPlanSegmentInputs())
        {
            if (segment_input->isStable())
            {
                unprunable_plan_segments.insert(node.getPlanSegment()->getPlanSegmentId());
            }
        }
    }
}

void SourcePruner::prepare()
{
    generateSegmentStorageMap();
    generateUnprunableSegments();
}

void SourcePruner::pruneSource(ContextPtr context, std::unordered_map<size_t, PlanSegment *> & id_to_segment)
{
    prepare();
    for (auto & node : plan_segments_ptr->getNodes())
    {
        auto plan_segment_id = node.getPlanSegment()->getPlanSegmentId();
        if (unprunable_plan_segments.contains(plan_segment_id))
            continue;

        if (!plan_segment_storages_map[plan_segment_id].empty())
        {
            //TODO: Worker map insert shard nodes
            // for (const auto & uuid : plan_segment_storages_map[plan_segment_id])
            // {
            //     const auto & target_workers = server_resource->getAssignedWorkers(uuid);
            //     plan_segment_workers_map[plan_segment_id].insert(target_workers.begin(), target_workers.end());
            // }
            plan_segment_workers_map[plan_segment_id];
            LOG_TRACE(
                log, "SourcePrune plan segment : {} worker size {}", plan_segment_id, plan_segment_workers_map[plan_segment_id].size());
        }
    }
    for (auto & node : plan_segments_ptr->getNodes())
    {
        auto plan_segment_id = node.getPlanSegment()->getPlanSegmentId();
        auto iter = plan_segment_workers_map.find(plan_segment_id);
        if (iter != plan_segment_workers_map.end())
        {
            auto parallel_size = iter->second.empty() ? 1 : iter->second.size();
            // Adjust the parallel size of pruned plan segment.
            node.getPlanSegment()->setParallelSize(parallel_size);
            auto inputs = node.plan_segment->getPlanSegmentInputs();
            for (auto & input : inputs)
            {
                auto child_iter = id_to_segment.find(input->getPlanSegmentId());
                if (child_iter != id_to_segment.end())
                {
                    for (auto & output : child_iter->second->getPlanSegmentOutputs())
                    {
                        if (output->getExchangeId() == input->getExchangeId())
                        {
                            // Adjust the parallel size of the input plan segment of the pruned segment.
                            output->setParallelSize(parallel_size);
                        }
                    }
                }
            }
        }
    }
}

PlanSegment * DAGGraph::getPlanSegmentPtr(size_t id)
{
    auto it = id_to_segment.find(id);
    if (it == id_to_segment.end())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: segment {} not found", id);
    }
    return it->second;
}

}
