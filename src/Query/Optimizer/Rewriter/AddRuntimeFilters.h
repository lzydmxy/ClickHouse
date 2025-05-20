#pragma once

#include <Common/Logger.h>
#include <Interpreters/Context.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/Optimizer/CardinalityEstimate/FilterEstimator.h>
#include <Query/Optimizer/CardinalityEstimate/SymbolStatistics.h>
#include <Query/Optimizer/Property/Property.h>
#include <Query/Optimizer/Rewriter/Rewriter.h>
#include <Query/Processors/QueryPlan/PlanVisitor.h>
#include <Query/Processors/QueryPlan/SimplePlanRewriter.h>
#include <Query/Processors/QueryPlan/SimplePlanVisitor.h>

namespace DB
{
/**
 * AddRuntimeFilters generates, analyze, merge and remove inefficient or unused runtime filters.
 *
 * Runtime Filter, also as dynamic filter, improve the performance of queries with selective joins
 * by filtering data early that would be filtered by join condition.
 *
 * When runtime Filtering is enabled, values are collected from the build side of join, and sent to
 * probe side of join in runtime.
 *
 * Runtime Filter could be used for dynamic partition pruning, reduce table scan data with index,
 * reduce exchange shuffle, and so on.
 *
 * Runtime Filter has two parts in plan:
 *  1. build side, model as join attribute.
 *  2. consumer side, model as a filter predicates.
 */
class AddRuntimeFilters : public Rewriter
{
public:
    String name() const override { return "AddRuntimeFilters"; }

private:
    bool isEnabled(ContextMutablePtr context) const override { return context->getOptimizerContext()->getSettingsRef().enable_runtime_filter; }
    bool rewrite(QueryPlanExt & plan, ContextMutablePtr context) const override;

    class AddRuntimeFilterRewriter;
    class RuntimeFilterInfoExtractor;
    class RemoveUnusedRuntimeFilterProbRewriter;
    class RemoveUnusedRuntimeFilterBuildRewriter;
    class AddExchange;
};

class AddRuntimeFilters::AddRuntimeFilterRewriter : public PlanNodeVisitor<PlanPropEquivalences, Void>
{
public:
    AddRuntimeFilterRewriter(ContextMutablePtr context_, CTEInfo & cte_info_)
        : context(std::move(context_)), cte_info(cte_info_), cte_helper(cte_info_)
    {
    }
    bool rewrite(QueryPlanExt & plan);
    PlanPropEquivalences visitPlanNode(PlanNodeBase & node, Void & c) override;
    PlanPropEquivalences visitJoinStepExtNode(JoinStepExtNode & node, Void & c) override;
    PlanPropEquivalences visitCTERefStepExtNode(CTERefStepExtNode & node, Void & c) override;

    PlanPropEquivalences replaceChildren(
        PlanNodeBase & node, PlanNodes children, std::vector<SymbolEquivalencesPtr> children_equivalences, PropertySet input_properties);

    RuntimeFilterId getId() const { return id; }

private:
    RuntimeFilterId nextId() { return id++; }

    RuntimeFilterId id = 0;
    ContextMutablePtr context;
    CTEInfo & cte_info;
    SimpleCTEVisitHelper<PlanPropEquivalences> cte_helper;
    LoggerPtr logger = getLogger("AddRuntimeFilters");
};

struct RuntimeFilterContext
{
    std::unordered_map<RuntimeFilterId, SymbolStatisticsPtr> runtime_filter_build_statistics;
    std::unordered_map<RuntimeFilterId, RuntimeFilterId> merged_runtime_filters;
    std::unordered_set<RuntimeFilterId> distributed_runtime_filters;
};

using InheritedRuntimeFilters = std::unordered_map<std::string, RuntimeFilterId>;

class AddRuntimeFilters::RuntimeFilterInfoExtractor : public PlanNodeVisitor<InheritedRuntimeFilters, std::unordered_set<RuntimeFilterId>>
{
public:
    static RuntimeFilterContext extract(QueryPlanExt & plan, ContextMutablePtr & context);

protected:
    RuntimeFilterInfoExtractor(ContextMutablePtr & context_, CTEInfo & cte_info_)
        : context(context_), cte_info(cte_info_), cte_helper(cte_info_)
    {
    }

    InheritedRuntimeFilters visitPlanNode(PlanNodeBase & node, std::unordered_set<RuntimeFilterId> & local_runtime_filter) override;
    InheritedRuntimeFilters visitProjectionStepExtNode(ProjectionStepExtNode & node, std::unordered_set<RuntimeFilterId> & local_runtime_filter) override;
    InheritedRuntimeFilters visitJoinStepExtNode(JoinStepExtNode & node, std::unordered_set<RuntimeFilterId> & local_runtime_filter) override;
    InheritedRuntimeFilters visitExchangeStepExtNode(ExchangeStepExtNode & node, std::unordered_set<RuntimeFilterId> & local_runtime_filter) override;
    InheritedRuntimeFilters visitCTERefStepExtNode(CTERefStepExtNode & node, std::unordered_set<RuntimeFilterId> & local_runtime_filter) override;
    InheritedRuntimeFilters visitFilterStepExtNode(FilterStepExtNode & node, std::unordered_set<RuntimeFilterId> & local_runtime_filter) override;

private:
    ContextMutablePtr context;
    CTEInfo & cte_info;
    SimpleCTEVisitHelper<InheritedRuntimeFilters> cte_helper;

    RuntimeFilterContext runtime_filter_context;
};

class AddRuntimeFilters::RemoveUnusedRuntimeFilterProbRewriter : public PlanNodeVisitor<PlanNodePtr, std::unordered_set<RuntimeFilterId>>
{
public:
    RemoveUnusedRuntimeFilterProbRewriter(
        ContextMutablePtr context, CTEInfo & cte_info, RuntimeFilterContext & runtime_filter_context);

    PlanNodePtr rewrite(const PlanNodePtr & plan);

    const std::unordered_set<RuntimeFilterId> & getEffectiveRuntimeFilters() const { return effective_runtime_filters; }

protected:
    PlanNodePtr visitPlanNode(PlanNodeBase & node, std::unordered_set<RuntimeFilterId> & allowed_runtime_filters) override;
    PlanNodePtr visitFilterStepExtNode(FilterStepExtNode & node, std::unordered_set<RuntimeFilterId> & allowed_runtime_filters) override;
    PlanNodePtr visitJoinStepExtNode(JoinStepExtNode & node, std::unordered_set<RuntimeFilterId> & allowed_runtime_filters) override;
    PlanNodePtr visitCTERefStepExtNode(CTERefStepExtNode & node, std::unordered_set<RuntimeFilterId> & allowed_runtime_filters) override;

private:
    ContextMutablePtr context;
    SimpleCTEVisitHelper<PlanNodePtr> cte_helper;

    RuntimeFilterContext & runtime_filter_context;
    std::unordered_set<RuntimeFilterId> effective_runtime_filters;
};

class AddRuntimeFilters::RemoveUnusedRuntimeFilterBuildRewriter : public PlanNodeVisitor<PlanNodePtr, Void>
{
public:
    explicit RemoveUnusedRuntimeFilterBuildRewriter(
        ContextMutablePtr & context_,
        CTEInfo & cte_info,
        const std::unordered_set<RuntimeFilterId> & effective_runtime_filters,
        const RuntimeFilterContext & runtime_filter_context);
    PlanNodePtr rewrite(PlanNodePtr & node);

protected:
    PlanNodePtr visitPlanNode(PlanNodeBase & node, Void &) override;
    PlanNodePtr visitJoinStepExtNode(JoinStepExtNode & node, Void &) override;
    PlanNodePtr visitCTERefStepExtNode(CTERefStepExtNode & node, Void & context) override;

    ContextMutablePtr & context;
    SimpleCTEVisitHelper<PlanNodePtr> cte_helper;
    const std::unordered_set<RuntimeFilterId> & effective_runtime_filters;
    const RuntimeFilterContext & runtime_filter_context;
};

class AddRuntimeFilters::AddExchange : public SimplePlanRewriter<std::unordered_set<RuntimeFilterId>>
{
public:
    static PlanNodePtr rewrite(const PlanNodePtr & node, ContextMutablePtr context, CTEInfo & cte_info);

protected:
    explicit AddExchange(ContextMutablePtr context_, CTEInfo & cte_info_) : SimplePlanRewriter(context_, cte_info_) { }
    PlanNodePtr visitPlanNode(PlanNodeBase & node, std::unordered_set<RuntimeFilterId> &) override;
    PlanNodePtr visitExchangeStepExtNode(ExchangeStepExtNode & node, std::unordered_set<RuntimeFilterId> &) override;
    PlanNodePtr visitFilterStepExtNode(FilterStepExtNode & node, std::unordered_set<RuntimeFilterId> &) override;
    PlanNodePtr visitJoinStepExtNode(JoinStepExtNode & node, std::unordered_set<RuntimeFilterId> &) override;
    PlanNodePtr visitCTERefStepExtNode(CTERefStepExtNode & node, std::unordered_set<RuntimeFilterId> &) override;
    PlanNodePtr visitBufferStepExtNode(BufferStepExtNode & node, std::unordered_set<RuntimeFilterId> &) override;
};

}
