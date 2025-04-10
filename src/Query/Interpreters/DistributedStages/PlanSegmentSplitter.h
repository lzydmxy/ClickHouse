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
    PlanSegmentResult visitExchangeStepExtNode(QueryPlanExt::Node * node, PlanSegmentVisitorContext & split_context) override;
    PlanSegmentResult visitCTERefStepExtNode(QueryPlanExt::Node * node, PlanSegmentVisitorContext & context) override;
    PlanSegmentResult visitTotalsHavingStepExtNode(QueryPlanExt::Node * node, PlanSegmentVisitorContext & context) override;
    // PlanSegmentResult visitExtremesStepExtNode(QueryPlanExt::Node * node, PlanSegmentVisitorContext & context) override;

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
    std::vector<std::optional<PartitioningHandle>> visitValuesStepExtNode(QueryPlanExt::Node * node, const Context & context) override;
    std::vector<std::optional<PartitioningHandle>> visitReadNothingStepNode(QueryPlanExt::Node * node, const Context & context) override;
    std::vector<std::optional<PartitioningHandle>> visitTableScanStepExtNode(QueryPlanExt::Node * node, const Context & context) override;
    std::vector<std::optional<PartitioningHandle>> visitRemoteExchangeSourceStepExtNode(QueryPlanExt::Node * node, const Context & context) override;
    std::vector<std::optional<PartitioningHandle>> visitExchangeStepExtNode(QueryPlanExt::Node * node, const Context & context) override;
    std::vector<std::optional<PartitioningHandle>> visitCTERefStepExtNode(QueryPlanExt::Node * node, const Context & context) override;
    std::vector<std::optional<PartitioningHandle>> visitReadStorageRowCountStepExtNode(QueryPlanExt::Node * node, const Context & context) override;

private:
    CTENodes & cte_nodes;
};

class SetScalable : public NodeVisitor<Void, const Context>
{
    SetScalable(bool scalable_, CTENodes & cte_nodes_) : scalable(scalable_), cte_nodes(cte_nodes_) { }

public:
    static void setScalable(QueryPlanExt::Node * node, CTENodes & cte_nodes, const Context & context);

    Void visitNode(QueryPlanExt::Node * node, const Context & context) override;
    Void visitExchangeStepExtNode(QueryPlanExt::Node * node, const Context & context) override;
    Void visitCTERefStepExtNode(QueryPlanExt::Node * node, const Context & context) override;

private:
    bool scalable;
    CTENodes & cte_nodes;
};

class ParallelSizeChecker: public NodeVisitor<std::vector<size_t>, const Context>
{
public:
    std::vector<size_t> visitNode(QueryPlanExt::Node * node, const Context & context) override;
    std::vector<size_t> visitValuesStepExtNode(QueryPlanExt::Node * node, const Context & context) override;
    std::vector<size_t> visitReadNothingStepNode(QueryPlanExt::Node * node, const Context & context) override;
    std::vector<size_t> visitTableScanStepExtNode(QueryPlanExt::Node * node, const Context & context) override;
    std::vector<size_t> visitRemoteExchangeSourceStepExtNode(QueryPlanExt::Node * node, const Context & context) override;
    std::vector<size_t> visitReadStorageRowCountStepExtNode(QueryPlanExt::Node * node, const Context & context) override;

    PlanSegment * segment;
    std::vector<PlanSegment *> children_segments;
    size_t shard_number;
};

}
