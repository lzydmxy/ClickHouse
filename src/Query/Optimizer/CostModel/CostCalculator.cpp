#include <Query/Optimizer/CostModel/CostCalculator.h>

#include <Query/Optimizer/CostModel/AggregatingCost.h>
#include <Query/Optimizer/CostModel/CTECost.h>
#include <Query/Optimizer/CostModel/ExchangeCost.h>
#include <Query/Optimizer/CostModel/FilterCost.h>
#include <Query/Optimizer/CostModel/JoinCost.h>
#include <Query/Optimizer/CostModel/ProjectionCost.h>
#include <Query/Optimizer/CostModel/TableScanCost.h>
#include <Query/Optimizer/CostModel/ValuesCost.h>
#include <Query/Optimizer/Cascades/CascadesOptimizer.h>

namespace DB
{

PlanNodeCost CostCalculator::calculatePlanCost(QueryPlanExt & plan, const Context & context)
{
    PlanCostMap plan_cost_map;
    if (!plan.getPlanNode()->getStatistics())
        return {};
    size_t worker_size = context.getOptimizerContext()->getWorkerSize();
    auto cte_ref_counts = plan.getCTEInfo().collectCTEReferenceCounts(plan.getPlanNode());
    PlanCostVisitor visitor{CostModel{context}, worker_size, plan.getCTEInfo(), cte_ref_counts};
    return VisitorUtil::accept(plan.getPlanNode(), visitor, plan_cost_map).cost;
}

PlanCostMap CostCalculator::calculate(QueryPlanExt & plan, const Context & context)
{
    PlanCostMap plan_cost_map;
    if (!plan.getPlanNode()->getStatistics())
        return plan_cost_map;
    size_t worker_size = context.getOptimizerContext()->getWorkerSize();
    auto cte_ref_counts = plan.getCTEInfo().collectCTEReferenceCounts(plan.getPlanNode());
    PlanCostVisitor visitor{CostModel{context}, worker_size, plan.getCTEInfo(), cte_ref_counts};
    VisitorUtil::accept(plan.getPlanNode(), visitor, plan_cost_map);
    return plan_cost_map;
}

PlanNodeCost CostCalculator::calculate(
    QueryPlanStepPtr & step,
    const PlanNodeStatisticsPtr & stats,
    const std::vector<PlanNodeStatisticsPtr> & children_stats,
    const Context & context,
    size_t worker_size)
{
    static CostVisitor visitor;
    CostContext cost_context{
        .cost_model = CostModel{context}, .stats = stats, .children_stats = children_stats, .worker_size = worker_size};
    return VisitorUtil::accept(step, visitor, cost_context);
}

PlanNodeCost CostVisitor::visitStep(const IQueryPlanStep &, CostContext &)
{
    return PlanNodeCost::ZERO;
}

PlanNodeCost CostVisitor::visitProjectionStepExt(const ProjectionStepExt & step, CostContext & context)
{
    return ProjectionCost::calculate(step, context);
}

PlanNodeCost CostVisitor::visitFilterStepExt(const FilterStepExt & step, CostContext & context)
{
    return FilterCost::calculate(step, context);
}

PlanNodeCost CostVisitor::visitJoinStepExt(const JoinStepExt & step, CostContext & cost_context)
{
    return JoinCost::calculate(step, cost_context);
}

PlanNodeCost CostVisitor::visitArrayJoinStep(const ArrayJoinStep & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitAggregatingStepExt(const AggregatingStepExt & step, CostContext & context)
{
    return AggregatingCost::calculate(step, context);
}

PlanNodeCost CostVisitor::visitReadStorageRowCountStepExt(const ReadStorageRowCountStepExt & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitWindowStep(const WindowStep & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitMergingAggregatedStepExt(const MergingAggregatedStepExt & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitUnionStepExt(const UnionStepExt & step, CostContext & context)
{
    return visitStep(step, context);
}

// PlanNodeCost CostVisitor::visitIntersectStep(const IntersectStep & step, CostContext & context)
// {
//     return visitStep(step, context);
// }
//
// PlanNodeCost CostVisitor::visitExceptStep(const ExceptStep & step, CostContext & context)
// {
//     return visitStep(step, context);
// }

PlanNodeCost CostVisitor::visitExchangeStepExt(const ExchangeStepExt & step, CostContext & cost_context)
{
    return ExchangeCost::calculate(step, cost_context);
}


PlanNodeCost CostVisitor::visitRemoteExchangeSourceStepExt(const RemoteExchangeSourceStepExt & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitTableScanStepExt(const TableScanStepExt & step, CostContext & context)
{
    return TableScanCost::calculate(step, context);
}

PlanNodeCost CostVisitor::visitReadNothingStep(const ReadNothingStep & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitValuesStepExt(const ValuesStepExt & step, CostContext & context)
{
    return ValuesCost::calculate(step, context);
}
PlanNodeCost CostVisitor::visitLimitStepExt(const LimitStepExt & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitLimitByStep(const LimitByStep & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitSortingStepExt(const SortingStepExt & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitMergeSortingStepExt(const MergeSortingStepExt & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitPartialSortingStepExt(const PartialSortingStepExt & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitMergingSortedStepExt(const MergingSortedStepExt & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitDistinctStepExt(const DistinctStepExt & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitExtremesStep(const ExtremesStep & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitApplyStepExt(const ApplyStepExt & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitEnforceSingleRowStepExt(const EnforceSingleRowStepExt & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitAssignUniqueIdStepExt(const AssignUniqueIdStepExt & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitCTERefStepExt(const CTERefStepExt & step, CostContext & context)
{
    return CTECost::calculate(step, context);
}

PlanNodeCost CostVisitor::visitExplainAnalyzeStepExt(const ExplainAnalyzeStepExt & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitTopNFilteringStepExt(const TopNFilteringStepExt & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitFillingStep(const FillingStep & step, CostContext & context)
{
    return visitStep(step, context);
}

// PlanNodeCost CostVisitor::visitIntermediateResultCacheStep(const IntermediateResultCacheStep & step, CostContext & context)
// {
//     return visitStep(step, context);
// }

CostWithCTEReferenceCounts PlanCostVisitor::visitPlanNode(PlanNodeBase & node, PlanCostMap & plan_cost_map)
{
    PlanNodeCost cost;
    std::unordered_map<CTEId, UInt64> cte_reference_counts;
    std::vector<PlanNodeStatisticsPtr> children_stats;
    for (auto & child : node.getChildren())
    {
        auto res = VisitorUtil::accept(*child, *this, plan_cost_map);
        cost += res.cost;
        for (auto & item : res.cte_reference_counts)
            cte_reference_counts[item.first] += item.second;
        children_stats.emplace_back(child->getStatistics().value_or(nullptr));
    }

    for (auto itr = cte_reference_counts.begin(); itr != cte_reference_counts.end();)
    {
        // lowest common ancestor for cte
        CTEId cte_id = itr->first;
        if (itr->second == cte_ref_counts.at(cte_id))
        {
            auto res = VisitorUtil::accept(*cte_info.getCTEDef(cte_id), *this, plan_cost_map);
            cost += res.cost;
            for (auto & item : res.cte_reference_counts)
                cte_reference_counts[item.first] += item.second;
            itr = cte_reference_counts.erase(itr);
        }
        else
            ++itr;
    }

    static CostVisitor visitor;
    CostContext cost_context{.cost_model = cost_model, .stats = node.getStatistics().value_or(nullptr),
                             .children_stats = children_stats, .worker_size = worker_size};
    cost += VisitorUtil::accept(node.getStep(), visitor, cost_context);
    plan_cost_map.emplace(node.getId(), cost.getCost(cost_model));
    return CostWithCTEReferenceCounts{cost, cte_reference_counts};
}

CostWithCTEReferenceCounts PlanCostVisitor::visitCTERefStepExtNode(CTERefStepExtNode & node, PlanCostMap & plan_cost_map)
{
    const auto * cte_step = dynamic_cast<const CTERefStepExt *>(node.getStep().get());
    auto res = visitPlanNode(node, plan_cost_map);
    res.cte_reference_counts[cte_step->getId()] += 1;
    return res;
}

}
