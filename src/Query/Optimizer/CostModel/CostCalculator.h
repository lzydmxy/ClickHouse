#pragma once

#include <Interpreters/Context.h>
#include <Query/Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Query/Optimizer/CostModel/CostModel.h>
#include <Query/Optimizer/CostModel/PlanNodeCost.h>
#include <Query/Processors/QueryPlan/PlanVisitor.h>
#include <Query/Processors/QueryPlan/QueryPlanExt.h>

namespace DB
{
using PlanCostMap = std::unordered_map<PlanNodeId, double>;

class CostCalculator
{
public:
    static PlanNodeCost calculatePlanCost(QueryPlanExt & plan, const Context & context);

    static PlanCostMap calculate(QueryPlanExt & plan, const Context & context);

    static PlanNodeCost calculate(
        QueryPlanStepExtPtr & step,
        const PlanNodeStatisticsPtr & stats,
        const std::vector<PlanNodeStatisticsPtr> & children_stats,
        const Context & context,
        size_t worker_size);
};

struct CostContext
{
    CostModel cost_model;
    PlanNodeStatisticsPtr stats;
    const std::vector<PlanNodeStatisticsPtr> & children_stats;
    size_t worker_size;
};

class CostVisitor : public StepVisitor<PlanNodeCost, CostContext>
{
public:
    PlanNodeCost visitStep(const IQueryPlanStep &, CostContext &) override;

    PlanNodeCost visitProjectionStepExt(const ProjectionStepExt & step, CostContext & context) override;
    PlanNodeCost visitFilterStepExt(const FilterStepExt & step, CostContext & context) override;
    PlanNodeCost visitJoinStepExt(const JoinStepExt & step, CostContext & cost_context) override;
    PlanNodeCost visitArrayJoinStep(const ArrayJoinStep & step, CostContext & cost_context) override;
    PlanNodeCost visitAggregatingStepExt(const AggregatingStepExt & step, CostContext & context) override;
    PlanNodeCost visitWindowStep(const WindowStep & step, CostContext & context) override;
    PlanNodeCost visitMergingAggregatedStepExt(const MergingAggregatedStepExt & step, CostContext & context) override;
    PlanNodeCost visitUnionStep(const UnionStep & step, CostContext & context) override;
    /// todo wujianchao add intersect and except
    // PlanNodeCost visitIntersectStep(const IntersectStep & step, CostContext & context) override;
    // PlanNodeCost visitExceptStep(const ExceptStep & step, CostContext & context) override;
    PlanNodeCost visitExchangeStepExt(const ExchangeStepExt & step, CostContext & cost_context) override;
    PlanNodeCost visitRemoteExchangeSourceStepExt(const RemoteExchangeSourceStepExt & step, CostContext & context) override;
    PlanNodeCost visitTableScanStepExt(const TableScanStepExt & step, CostContext & context) override;
    PlanNodeCost visitReadNothingStep(const ReadNothingStep & step, CostContext & context) override;
    PlanNodeCost visitValuesStepExt(const ValuesStepExt & step, CostContext & context) override;
    PlanNodeCost visitLimitStep(const LimitStep & step, CostContext & context) override;
    PlanNodeCost visitLimitByStep(const LimitByStep & step, CostContext & context) override;
    PlanNodeCost visitSortingStepExt(const SortingStepExt & step, CostContext & context) override;
    PlanNodeCost visitMergeSortingStepExt(const MergeSortingStepExt & step, CostContext & context) override;
    PlanNodeCost visitPartialSortingStepExt(const PartialSortingStepExt & step, CostContext & context) override;
    PlanNodeCost visitMergingSortedStepExt(const MergingSortedStepExt & step, CostContext & context) override;
    PlanNodeCost visitDistinctStepExt(const DistinctStepExt & step, CostContext & context) override;
    PlanNodeCost visitExtremesStep(const ExtremesStep & step, CostContext & context) override;
    PlanNodeCost visitApplyStepExt(const ApplyStepExt & step, CostContext & context) override;
    PlanNodeCost visitEnforceSingleRowStepExt(const EnforceSingleRowStepExt & step, CostContext & context) override;
    PlanNodeCost visitAssignUniqueIdStepExt(const AssignUniqueIdStepExt & step, CostContext & context) override;
    PlanNodeCost visitCTERefStepExt(const CTERefStepExt & step, CostContext & context) override;
    PlanNodeCost visitExplainAnalyzeStepExt(const ExplainAnalyzeStepExt & step, CostContext & context) override;
    PlanNodeCost visitTopNFilteringStepExt(const TopNFilteringStepExt & step, CostContext & context) override;
    PlanNodeCost visitFillingStep(const FillingStep & step, CostContext & context) override;
    PlanNodeCost visitReadStorageRowCountStepExt(const ReadStorageRowCountStepExt & step, CostContext & context) override;
    // PlanNodeCost visitIntermediateResultCacheStepExt(const IntermediateResultCacheStepExt & step, CostContext & context) override;
};

struct CostWithCTEReferenceCounts
{
    PlanNodeCost cost;
    std::unordered_map<CTEId, UInt64> cte_reference_counts;
};

class PlanCostVisitor : public PlanNodeVisitor<CostWithCTEReferenceCounts, PlanCostMap>
{
public:
    PlanCostVisitor(
        CostModel cost_model_, size_t worker_size_, CTEInfo & cte_info_, const std::unordered_map<CTEId, UInt64> & cte_ref_counts_)
        : cost_model(std::move(cost_model_)), worker_size(worker_size_), cte_info(cte_info_), cte_ref_counts(cte_ref_counts_)
    {
    }

    CostWithCTEReferenceCounts visitPlanNode(PlanNodeBase &, PlanCostMap & map) override;
    CostWithCTEReferenceCounts visitCTERefNode(CTERefNode & node, PlanCostMap & map) override;

private:
    CostModel cost_model;
    size_t worker_size;
    CTEInfo & cte_info;
    const std::unordered_map<CTEId, UInt64> & cte_ref_counts;
};


}
