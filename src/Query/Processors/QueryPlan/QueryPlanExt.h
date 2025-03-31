#pragma once

#include <Interpreters/StorageID.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Query/Processors/QueryPlan/CTEInfo.h>
#include <Query/Processors/QueryPlan/PlanNodeIdAllocator.h>

namespace DB
{

class ReadBuffer;

class QueryPlanExt;
using QueryPlanExtPtr = std::shared_ptr<QueryPlanExt>;

class PlanNodeBase;
using PlanNodePtr = std::shared_ptr<PlanNodeBase>;
using PlanNodes = std::vector<PlanNodePtr>;

namespace Protos
{
class QueryPlanExt;
}

/// A tree of query steps.
/// The goal of QueryPlanExt is to build QueryPipeline.
/// QueryPlanExt let delay pipeline creation which is helpful for pipeline-level optimizations.
class QueryPlanExt : public QueryPlan
{
public:
    QueryPlanExt() {}
    ~QueryPlanExt() {}
    QueryPlanExt(QueryPlanExt &&) noexcept;
    QueryPlanExt(PlanNodePtr root_, PlanNodeIdAllocatorPtr id_allocator_);
    QueryPlanExt(PlanNodePtr root_, CTEInfo cte_info, PlanNodeIdAllocatorPtr id_allocator_);

    QueryPlanExt & operator=(QueryPlanExt &&) noexcept;

    std::set<StorageID> allocateLocalTable(ContextPtr context);
    PlanNodeIdAllocatorPtr & getIdAllocator() { return id_allocator; }
    void createIdAllocator() { id_allocator = std::make_shared<PlanNodeIdAllocator>(); }
    void update(PlanNodePtr plan) { plan_node = std::move(plan); }

    void unitePlans(QueryPlanStepPtr step, std::vector<QueryPlanExtPtr> plans);
    void addStep(QueryPlanStepPtr step, PlanNodes children = {});

    QueryPipelineBuilderPtr buildQueryPipeline(
        const QueryPlanOptimizationSettings & optimization_settings, const BuildQueryPipelineSettings & build_pipeline_settings) { return nullptr;}

    /// add step_id for processors
    static void updatePipelineStepInfo(QueryPipelineBuilderPtr & pipeline_ptr, QueryPlanStepPtr & step, size_t step_id);
    /// If initialized, build pipeline and convert to pipe. Otherwise, return empty pipe.
    Pipe
    convertToPipe(const QueryPlanOptimizationSettings & optimization_settings, const BuildQueryPipelineSettings & build_pipeline_settings);

    void explainPipeline(WriteBuffer & buffer, const ExplainPipelineOptions & options) const;
    void explainPipelineWithOptimizer(WriteBuffer & buffer, const ExplainPipelineOptions & options) const;

    void setShortCircuit(bool short_circuit_) { short_circuit = short_circuit_; }
    bool isShortCircuit() const { return short_circuit; }

    void addInterpreterContext(std::shared_ptr<Context> context) {}

    std::unordered_map<const Node *, size_t> node_id_map;
    size_t getNodeId(const Node * node);

    using CTEId = UInt32;
    using CTENodes = std::unordered_map<CTEId, Node *>;

    Nodes & getNodes() { return nodes; }
    const Nodes & getNodes() const { return nodes; }

    Node * getRoot() { return root; }
    const Node * getRoot() const { return root; }
    void setRoot(Node * root_) { root = root_; }
    CTENodes & getCTENodes() { return cte_nodes; }

    Node * getLastNode() { return &nodes.back(); }

    void addNode(QueryPlan::Node && node_, size_t id);

    void addRoot(QueryPlan::Node && node_, size_t id);
    UInt32 newPlanNodeId() { return (*max_node_id)++; }
    PlanNodePtr & getPlanNode() { return plan_node; }
    PlanNodePtr getPlanNode() const { return plan_node; }
    void setPlanNode(PlanNodePtr new_plan_node) { plan_node = std::move(new_plan_node); }
    CTEInfo & getCTEInfo() { return cte_info; }
    const CTEInfo & getCTEInfo() const { return cte_info; }
    PlanNodePtr getPlanNodeById(PlanNodeId node_id) const;
    static UInt32 getPlanNodeCount(PlanNodePtr node);

    QueryPlanExt getSubPlan(QueryPlan::Node * node_);

    // TODO: implement
    // void toProto(Protos::QueryPlanExt & proto) const;
    // void fromProto(const Protos::QueryPlanExt & proto);

    // // handle when plan is tree-like, i.e., plan_node + cte_info
    // void toProtoTreeLike(Protos::QueryPlanExt & proto) const;
    // void fromProtoTreeLike(const Protos::QueryPlanExt & proto);

    // // handle when plan is flatten, i.e., root + nodes + cte_nodes
    // void toProtoFlatten(Protos::QueryPlanExt & proto) const;
    // void fromProtoFlatten(const Protos::QueryPlanExt & proto);

    void freshPlan();

    size_t getSize() const { return nodes.size(); }

    void setResetStepId(bool reset_id) { reset_step_id = reset_id; }

    QueryPlanExtPtr copy(ContextMutablePtr context);

private:
    LoggerPtr log = getLogger("QueryPlanExt");

    // Flatten, in segment only
    // Nodes nodes;
    CTENodes cte_nodes; // won't serialize
    // Node * root = nullptr;

    // Tree-Like, for optimizer
    PlanNodePtr plan_node = nullptr;
    CTEInfo cte_info;

    PlanNodeIdAllocatorPtr id_allocator;

    /// Those fields are passed to QueryPipeline.
    // size_t max_threads = 0;
    std::vector<std::shared_ptr<Context>> interpreter_context;
    std::shared_ptr<UInt32> max_node_id;
    // Whether reset step id in serialize()，use for explain analyze.
    bool reset_step_id = true;

    bool short_circuit = false;
};

}
