#pragma once
#include <set>
#include <string_view>
#include <unordered_map>
#include <time.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <Client/Connection.h>
#include <Common/Macros.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
//#include <IO/ConnectionTimeoutsContext.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/queryToString.h>
#include <butil/endpoint.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Query/Common/StringUtils.h>
#include <Query/ProtosHelper/HostWithPorts.h>
#include <Query/ProtosHelper/QueryProto.h>
#include <Query/ProtosHelper/AddressInfo.h>
#include <Query/ProtosHelper/SourceTask.h>
#include <Query/Executor/DAGGraph.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/Executor/PlanSegmentInstance.h>
#include <Query/Executor/sendPlanSegment.h>


namespace DB
{
enum class NodeSelectorPolicy : int8_t
{
    InvalidPolicy,
    LocalPolicy,
    SourcePolicy,
    ComputePolicy
};

enum class NodeType : uint8_t
{
    Local,
    Remote
};

struct SourceTaskPayload
{
    size_t rows = 0;
    size_t part_num = 0;
    std::set<Int64> buckets;
    String toString() const
    {
        return fmt::format("SourceTaskPayload(buckets:[{}],rows:{},part_num:{})",
            //boost::join(buckets | boost::adaptors::transformed([](Int64 b) { return std::to_string(b); }), ","),
            setToString<Int64>(buckets)
            , rows, part_num);
    }
};

struct SourceTaskPayloadOnWorker
{
    String worker_id;
    size_t rows = 0;
    size_t part_num = 0;
    /// Bucket group is the minimum granularity to schedule source task, below is an example constructing bucket groups
    /// suppose we have two tables t1 and t2. t1's max bucket number is 4, t2's max bucket number is 8.
    /// t1 has buckets 0, 1, 2, 3 and t2 has buckets 0,1,2,3,4,5,6,7
    /// then we have bucket groups {0, 4}, {1, 5}, {2, 6}, {3, 7}
    std::map<Int64, std::set<Int64>> bucket_groups{};
};

/// Node of a replica of a shard
struct WorkerNode
{
    WorkerNode() = default;
    explicit WorkerNode(const AddressInfo & address_, NodeType type_ = NodeType::Remote, String id_ = "")
        : address(address_), type(type_), id(id_)
    {
        if (id == "")
            id = address.toShortString();
    }
    AddressInfo address;
    NodeType type{NodeType::Remote};
    String id;
};
using WorkerNodes = std::vector<WorkerNode>;

using ClusterPtr = std::shared_ptr<Cluster>;

class ClusterNodes
{
public:    
    ClusterNodes() = default;
    explicit ClusterNodes(const std::string cluster_name_, ContextPtr & query_context);
    String cluster_name;
    ClusterPtr cluster;
    std::vector<size_t> rank_worker_ids;
    std::vector<WorkerNode> all_workers;
    HostWithPortsVec all_hosts;
    void initWorkersIndex();
};

struct NodeSelectorResult;

void divideSourceTaskByBucket(
    std::unordered_map<AddressInfo, SourceTaskPayloadOnWorker, AddressInfo::Hash> & payloads,
    size_t weight_sum,
    size_t parallel_size,
    NodeSelectorResult & result);
void divideSourceTaskByPart(
    std::unordered_map<AddressInfo, SourceTaskPayloadOnWorker, AddressInfo::Hash> & payloads,
    size_t weight_sum,
    size_t parallel_size,
    NodeSelectorResult & result);

struct NodeSelectorResult
{
    WorkerNodes worker_nodes;
    std::vector<size_t> indexes{};
    std::unordered_map<AddressInfo, size_t, AddressInfo::Hash> source_task_count_on_workers{};
    std::unordered_map<AddressInfo, std::vector<std::set<Int64>>, AddressInfo::Hash> buckets_on_workers{};

    //input plansegment id => source address and partition ids, ordered by parallel index, used by bsp mode
    std::map<PlanSegmentInstanceID, std::vector<PlanSegmentPartitionSource>> sources{};
    //exchange_id => source_address, used by mpp mode
    std::unordered_map<size_t, std::vector<std::shared_ptr<AddressInfo>>> source_addresses{};

    AddressInfos getAddress() const
    {
        AddressInfos ret;
        ret.reserve(worker_nodes.size());
        for (const auto & address : worker_nodes)
            ret.emplace_back(address.address);
        return ret;
    }

    String toString() const;
};

///////////////node selector

template <class DerivedSelector>
class CommonNodeSelector
{
public:
    CommonNodeSelector(const ClusterNodes & cluster_nodes_, LoggerPtr log_) : cluster_nodes(cluster_nodes_), log(log_) { }

    void checkClusterInfo(PlanSegment * plan_segment_ptr)
    {
        if (plan_segment_ptr->getClusterName().empty())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: can't find workgroup in context which named {}",
                plan_segment_ptr->getClusterName());
        }
    }

    bool needStableSchedule(PlanSegment * plan_segment_ptr)
    {
        const auto & inputs = plan_segment_ptr->getPlanSegmentInputs();
        return std::any_of(inputs.begin(), inputs.end(), [](const auto & input) { return input->isStable(); });
    }
    void
    selectPrunedWorkers(DAGGraph * dag_graph_ptr, PlanSegment * plan_segment_ptr, NodeSelectorResult & result, AddressInfo & local_address)
    {
        const auto & target_hosts = dag_graph_ptr->source_pruner->plan_segment_workers_map[plan_segment_ptr->getPlanSegmentId()];
        if (target_hosts.empty())
        {
            LOG_DEBUG(log, "SourcePrune plan segment {} select first worker.", plan_segment_ptr->getPlanSegmentId());
            for (const auto & worker : cluster_nodes.all_workers)
            {
                if (worker.address != local_address)
                {
                    result.worker_nodes.emplace_back(worker);
                    break;
                }
            }
        }
        else
        {
            LOG_DEBUG(log, "SourcePrune plan segment {} select workers after source prune.", plan_segment_ptr->getPlanSegmentId());
            for (size_t idx = 0; idx < cluster_nodes.rank_worker_ids.size(); idx++)
            {
                if (target_hosts.contains(cluster_nodes.all_hosts[idx]))
                    result.worker_nodes.emplace_back(cluster_nodes.all_workers[idx]);
            }
        }
    }
    ~CommonNodeSelector() = default;

protected:
    const ClusterNodes & cluster_nodes;
    LoggerPtr log;
};

class LocalNodeSelector : public CommonNodeSelector<LocalNodeSelector>
{
public:
    LocalNodeSelector(const ClusterNodes & cluster_nodes_, LoggerPtr log_) : CommonNodeSelector(cluster_nodes_, log_) { }
    NodeSelectorResult select(PlanSegment * plan_segment_ptr, ContextPtr query_context);
};

class LocalityNodeSelector : public CommonNodeSelector<LocalityNodeSelector>
{
public:
    LocalityNodeSelector(const ClusterNodes & cluster_nodes_, LoggerPtr log_) : CommonNodeSelector(cluster_nodes_, log_) { }
    NodeSelectorResult select(PlanSegment * plan_segment_ptr, ContextPtr query_context, DAGGraph * dag_graph_ptr);
};

class SourceNodeSelector : public CommonNodeSelector<SourceNodeSelector>
{
public:
    using Map = std::unordered_map<UUID, std::unordered_map<AddressInfo, SourceTaskPayload, AddressInfo::Hash>>;
    SourceNodeSelector(const ClusterNodes & cluster_nodes_, LoggerPtr log_) : CommonNodeSelector(cluster_nodes_, log_) { }
    NodeSelectorResult select(PlanSegment * plan_segment_ptr, ContextPtr query_context, DAGGraph * dag_graph_ptr);
};

class ComputeNodeSelector : public CommonNodeSelector<ComputeNodeSelector>
{
public:
    ComputeNodeSelector(const ClusterNodes & cluster_nodes_, LoggerPtr log_) : CommonNodeSelector(cluster_nodes_, log_) { }
    NodeSelectorResult select(PlanSegment * plan_segment_ptr, ContextPtr query_context, DAGGraph * dag_graph_ptr);
};

// TODO: More elegant: NodeSelector should be a super class and the rests should be its sub classes.
class NodeSelector
{
public:
    using SelectorResultMap = std::unordered_map<size_t, NodeSelectorResult>;
    NodeSelector(ClusterNodes & cluster_nodes_, ContextPtr query_context_, std::shared_ptr<DAGGraph> dag_graph_ptr_)
        : query_context(query_context_)
        , optimizer_context(query_context_->getOptimizerContext())
        , dag_graph_ptr(dag_graph_ptr_.get())
        , cluster_nodes(cluster_nodes_)
        , log(getLogger("NodeSelector"))
        , local_node_selector(cluster_nodes, log)
        , source_node_selector(cluster_nodes, log)
        , compute_node_selector(cluster_nodes, log)
        , locality_node_selector(cluster_nodes, log)
    {
    }

    NodeSelectorResult select(PlanSegment * plan_segment_ptr, bool has_table_scan);
    void setSources(
        PlanSegment * plan_segment_ptr,
        NodeSelectorResult * result,
        std::map<PlanSegmentInstanceID, std::vector<UInt32>> & read_partitions);
    /// this method would generate a mapping of instance_id => [exchange_id, partition ids],
    /// partition ids contain all partition a plan segment instance should read from.
    std::map<PlanSegmentInstanceID, std::vector<UInt32>> splitReadPartitions(PlanSegment * plan_segment_ptr);
    static std::map<PlanSegmentInstanceID, std::vector<UInt32>> coalescePartitions(
        UInt32 segment_id,
        size_t disk_shuffle_advisory_partition_size,
        size_t broadcast_partition,
        std::vector<size_t> & normal_partitions);
    static PlanSegmentInputPtr tryGetLocalInput(PlanSegment * plan_segment_ptr);

private:
    ContextPtr query_context;
    OptimizerContextPtr optimizer_context;
    DAGGraph * dag_graph_ptr;
    const ClusterNodes & cluster_nodes;
    LoggerPtr log;
    LocalNodeSelector local_node_selector;
    SourceNodeSelector source_node_selector;
    ComputeNodeSelector compute_node_selector;
    LocalityNodeSelector locality_node_selector;
};

}
