#pragma once

#include <optional>

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/QueryPriorities.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Query/Processors/QueryPlan/QueryPlanExt.h>
#include <Query/Processors/QueryPlan/ExchangeStepExt.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/Processors/IQueryPlanStepExt.h>
#include <Query/Processors/QueryPlan/PlanVisitor.h>


namespace DB
{

using CTEId = UInt32;
using CTENodes = std::unordered_map<CTEId, QueryPlanExt::Node *>;


using PlanSegmentResult = QueryPlanExt::Node *;

class PlanSegmentInput;
using PlanSegmentInputPtr = std::shared_ptr<PlanSegmentInput>;
using PlanSegmentInputs = std::vector<PlanSegmentInputPtr>;
struct PlanSegmentContext;

using PartitioningHandle = RPartitioningHandle;
using Component = RPartitioningComponent;

class PlanSegmentSplitter
{
public:
    static void split(QueryPlanExt & query_plan, PlanSegmentContext & plan_segment_context);
};

class Void
{
};

struct PlanSegmentContext
{
    ContextMutablePtr context;
    QueryPlanExt & query_plan;
    String query_id;
    size_t id = 0;
    size_t shard_number = 0;
    String cluster_name;
    PlanSegmentTree * plan_segment_tree;
    size_t getSegmentId() { return id++; }
};

struct PlanSegmentVisitorContext
{
    PlanSegmentInputs inputs;
    std::vector<PlanSegment *> children;
    size_t & exchange_id;
    String hash_func;
    Array params = Array();
    bool is_add_totals = false;
    bool is_add_extremes = false;
    bool scalable = true;
};

class PlanSegmentVisitor : public NodeVisitor<PlanSegmentResult, PlanSegmentVisitorContext>
{
public:
    explicit PlanSegmentVisitor(PlanSegmentContext & plan_segment_context_, CTENodes & cte_nodes_)
        : plan_segment_context(plan_segment_context_), cte_nodes(cte_nodes_)
    {
    }

    PlanSegmentResult visitNode(QueryPlanExt::Node *, PlanSegmentVisitorContext & split_context) override;
    PlanSegmentResult visitExchangeNode(QueryPlanExt::Node * node, PlanSegmentVisitorContext & split_context) override;
    PlanSegmentResult visitCTERefNode(QueryPlanExt::Node * node, PlanSegmentVisitorContext & context) override;
    PlanSegmentResult visitTotalsHavingNode(QueryPlanExt::Node * node, PlanSegmentVisitorContext & context) override;
    PlanSegmentResult visitExtremesNode(QueryPlanExt::Node * node, PlanSegmentVisitorContext & context) override;

    PlanSegment * createPlanSegment(QueryPlanExt::Node * node, PlanSegmentVisitorContext & split_context);
    PlanSegment * createPlanSegment(QueryPlanExt::Node * node, size_t segment_id, PlanSegmentVisitorContext & split_context);

private:
    PlanSegmentResult visitChild(QueryPlanExt::Node * node, PlanSegmentVisitorContext & split_context);
    PlanSegmentInputs findInputs(QueryPlanExt::Node * node);
    std::pair<String, size_t> findClusterAndParallelSize(QueryPlanExt::Node * node, PlanSegmentVisitorContext & split_context);

    PlanSegmentContext & plan_segment_context;
    CTENodes & cte_nodes;
    std::unordered_map<CTEId, std::pair<PlanSegment *, ExchangeStepExt *>> cte_plan_segments{};
};

class SourceNodeFinder : public NodeVisitor<std::vector<std::optional<PartitioningHandle>>, const Context>
{
public:
    explicit SourceNodeFinder(CTENodes & cte_nodes_) : cte_nodes(cte_nodes_) { }
    static std::vector<PartitioningHandle> find(QueryPlanExt::Node * node, CTENodes & cte_nodes, const Context & context);

    std::vector<std::optional<PartitioningHandle>> visitNode(QueryPlanExt::Node * node, const Context & context) override;
    std::vector<std::optional<PartitioningHandle>> visitValuesNode(QueryPlanExt::Node * node, const Context & context) override;
    std::vector<std::optional<PartitioningHandle>> visitReadNothingNode(QueryPlanExt::Node * node, const Context & context) override;
    std::vector<std::optional<PartitioningHandle>> visitTableScanNode(QueryPlanExt::Node * node, const Context & context) override;
    std::vector<std::optional<PartitioningHandle>> visitRemoteExchangeSourceNode(QueryPlanExt::Node * node, const Context & context) override;
    std::vector<std::optional<PartitioningHandle>> visitExchangeNode(QueryPlanExt::Node * node, const Context & context) override;
    std::vector<std::optional<PartitioningHandle>> visitCTERefNode(QueryPlanExt::Node * node, const Context & context) override;
    std::vector<std::optional<PartitioningHandle>> visitReadStorageRowCountNode(QueryPlanExt::Node * node, const Context & context) override;

private:
    CTENodes & cte_nodes;
};

class SetScalable : public NodeVisitor<Void, const Context>
{
    SetScalable(bool scalable_, CTENodes & cte_nodes_) : scalable(scalable_), cte_nodes(cte_nodes_) { }

public:
    static void setScalable(QueryPlanExt::Node * node, CTENodes & cte_nodes, const Context & context);

    Void visitNode(QueryPlanExt::Node * node, const Context & context) override;
    Void visitExchangeNode(QueryPlanExt::Node * node, const Context & context) override;
    Void visitCTERefNode(QueryPlanExt::Node * node, const Context & context) override;

private:
    bool scalable;
    CTENodes & cte_nodes;
};

class ParallelSizeChecker: public NodeVisitor<std::vector<size_t>, const Context>
{
public:
    std::vector<size_t> visitNode(QueryPlanExt::Node * node, const Context & context) override;
    std::vector<size_t> visitValuesNode(QueryPlanExt::Node * node, const Context & context) override;
    std::vector<size_t> visitReadNothingNode(QueryPlanExt::Node * node, const Context & context) override;
    std::vector<size_t> visitTableScanNode(QueryPlanExt::Node * node, const Context & context) override;
    std::vector<size_t> visitRemoteExchangeSourceNode(QueryPlanExt::Node * node, const Context & context) override;
    std::vector<size_t> visitReadStorageRowCountNode(QueryPlanExt::Node * node, const Context & context) override;

    PlanSegment * segment;
    std::vector<PlanSegment *> children_segments;
    size_t shard_number;
};

}
