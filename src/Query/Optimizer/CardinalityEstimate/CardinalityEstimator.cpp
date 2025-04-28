#include <Query/Optimizer/CardinalityEstimate/CardinalityEstimator.h>

#include <Query/Optimizer/CardinalityEstimate/AggregateEstimator.h>
#include <Query/Optimizer/CardinalityEstimate/AssignUniqueIdEstimator.h>
#include <Query/Optimizer/CardinalityEstimate/EnforceSingleRowEstimator.h>
#include <Query/Optimizer/CardinalityEstimate/ExchangeEstimator.h>
#include <Query/Optimizer/CardinalityEstimate/FilterEstimator.h>
#include <Query/Optimizer/CardinalityEstimate/JoinEstimator.h>
#include <Query/Optimizer/CardinalityEstimate/LimitEstimator.h>
#include <Query/Optimizer/CardinalityEstimate/ProjectionEstimator.h>
#include <Query/Optimizer/CardinalityEstimate/SampleEstimator.h>
#include <Query/Optimizer/CardinalityEstimate/SortingEstimator.h>
#include <Query/Optimizer/CardinalityEstimate/TableScanEstimator.h>
#include <Query/Optimizer/CardinalityEstimate/UnionEstimator.h>
#include <Query/Optimizer/CardinalityEstimate/WindowEstimator.h>
#include <Query/Optimizer/PredicateUtils.h>
#include <Query/Processors/QueryPlan/MergeSortingStepExt.h>
#include <Query/Processors/QueryPlan/MergingSortedStepExt.h>
#include <Query/Processors/QueryPlan/PartialSortingStepExt.h>
#include <Query/Processors/QueryPlan/QueryPlanExt.h>
#include <Query/Functions/InternalFunctionRuntimeFilter.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED; // NOLINT
}

std::optional<PlanNodeStatisticsPtr> CardinalityEstimator::estimate(
    QueryPlanStepPtr & step,
    CTEInfo & cte_info,
    std::vector<PlanNodeStatisticsPtr> children_stats,
    ContextMutablePtr context,
    bool simple_children,
    std::vector<bool> children_are_table_scan,
    std::vector<double> children_filter_selectivity,
    const InclusionDependency & inclusion_dependency)
{
    static CardinalityVisitor visitor;
    CardinalityContext cardinality_context{
        .context = context,
        .cte_info = cte_info,
        .children_stats = std::move(children_stats),
        .simple_children = simple_children,
        .children_are_table_scan = std::move(children_are_table_scan),
        .children_filter_selectivity = std::move(children_filter_selectivity),
        .inclusion_dependency = inclusion_dependency};
    auto stats = VisitorUtil::accept(step, visitor, cardinality_context);
    if (stats)
        stats->pruneSymbols(step->getOutputStream().header.getNameSet());
    return stats ? std::make_optional(stats) : std::nullopt;
}

std::optional<PlanNodeStatisticsPtr>
CardinalityEstimator::estimate(PlanNodeBase & node, CTEInfo & cte_info, ContextMutablePtr context, bool recursive, bool re_estimate)
{
    auto statistics = node.getStatistics();
    if (statistics.isDerived() && !recursive)
        return statistics.getStatistics();

    PlanCardinalityVisitor visitor{cte_info};
    CardinalityContext cardinality_context{.context = context, .cte_info = cte_info, .children_stats = {}, .re_estimate = re_estimate};
    auto stats = VisitorUtil::accept(node, visitor, cardinality_context);
    if (stats)
        stats->pruneSymbols(node.getCurrentDataStream().header.getNameSet());
    return stats ? std::make_optional(stats) : std::nullopt;
}

void CardinalityEstimator::estimate(QueryPlanExt & node, ContextMutablePtr context, bool re_estimate)
{
    estimate(*node.getPlanNode(), node.getCTEInfo(), context, true, re_estimate);
}

PlanNodeStatisticsPtr CardinalityVisitor::visitStep(const IQueryPlanStep &, CardinalityContext & context)
{
    //    throw Exception("Not impl card estimate", ErrorCodes::NOT_IMPLEMENTED);
    return context.children_stats[0];
}

PlanNodeStatisticsPtr CardinalityVisitor::visitOffsetStep(const OffsetStep & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = LimitEstimator::estimate(child_stats, step);
    return stats;
}

// PlanNodeStatisticsPtr CardinalityVisitor::visitTableFinishStep(const TableFinishStep &, CardinalityContext & context)
// {
//     PlanNodeStatisticsPtr child_stats = context.children_stats[0];
//     return child_stats;
//
// }
PlanNodeStatisticsPtr CardinalityVisitor::visitBufferStepExt(const BufferStepExt &, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;

}
PlanNodeStatisticsPtr CardinalityVisitor::visitMarkDistinctStepExt(const MarkDistinctStepExt &, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;

}
PlanNodeStatisticsPtr CardinalityVisitor::visitIntersectOrExceptStep(const IntersectOrExceptStep &, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;
}
// PlanNodeStatisticsPtr CardinalityVisitor::visitTableWriteStep(const TableWriteStep &, CardinalityContext & context)
// {
//     PlanNodeStatisticsPtr child_stats = context.children_stats[0];
//     return child_stats;
// }

// PlanNodeStatisticsPtr CardinalityVisitor::visitOutfileWriteStep(const OutfileWriteStep &, CardinalityContext & context)
// {
//     PlanNodeStatisticsPtr child_stats = context.children_stats[0];
//     return child_stats;
// }

// PlanNodeStatisticsPtr CardinalityVisitor::visitOutfileFinishStep(const OutfileFinishStep &, CardinalityContext & context)
// {
//     PlanNodeStatisticsPtr child_stats = context.children_stats[0];
//     return child_stats;
// }

PlanNodeStatisticsPtr CardinalityVisitor::visitFinalSampleStepExt(const FinalSampleStepExt & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return SampleEstimator::estimate(child_stats, step);
}

PlanNodeStatisticsPtr CardinalityVisitor::visitLocalExchangeStepExt(const LocalExchangeStepExt &, CardinalityContext & context)
{
    return context.children_stats[0];
}

PlanNodeStatisticsPtr CardinalityVisitor::visitProjectionStepExt(const ProjectionStepExt & step, CardinalityContext & context)
{
    if (context.children_stats.empty())
    {
        return std::make_shared<PlanNodeStatistics>(0);
    }

    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = ProjectionEstimator::estimate(child_stats, step);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitFilterStepExt(const FilterStepExt & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = FilterEstimator::estimate(
        child_stats, step.getFilter(), step.getInputStreams()[0].header.getNamesToTypes(), context.context, context.simple_children);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitJoinStepExt(const JoinStepExt & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr left_child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr right_child_stats = context.children_stats[1];
    PlanNodeStatisticsPtr stats = JoinEstimator::estimate(
        left_child_stats,
        right_child_stats,
        step,
        context.context,
        context.children_are_table_scan[0],
        context.children_are_table_scan[1],
        context.children_filter_selectivity,
        context.inclusion_dependency);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitArrayJoinStep(const ArrayJoinStep &, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitAggregatingStepExt(const AggregatingStepExt & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = AggregateEstimator::estimate(child_stats, step, context.context);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitWindowStep(const WindowStep & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = WindowEstimator::estimate(child_stats, step);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitMergingAggregatedStepExt(const MergingAggregatedStepExt & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = AggregateEstimator::estimate(child_stats, step, context.context);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitUnionStepExt(const UnionStepExt & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr stats = UnionEstimator::estimate(context.children_stats, step);
    return stats;
}

// PlanNodeStatisticsPtr CardinalityVisitor::visitIntersectStep(const IntersectStep &, CardinalityContext & context)
// {
//     PlanNodeStatisticsPtr child_stats = context.children_stats[0];
//     return child_stats;
// }

// PlanNodeStatisticsPtr CardinalityVisitor::visitExceptStep(const ExceptStep &, CardinalityContext & context)
// {
//     PlanNodeStatisticsPtr child_stats = context.children_stats[0];
//     return child_stats;
// }

PlanNodeStatisticsPtr CardinalityVisitor::visitExchangeStepExt(const ExchangeStepExt & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr stats = ExchangeEstimator::estimate(context.children_stats, step);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitRemoteExchangeSourceStepExt(const RemoteExchangeSourceStepExt &, CardinalityContext &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "RemoteExchangeSourceNode should not run here");
}

PlanNodeStatisticsPtr CardinalityVisitor::visitTableScanStepExt(const TableScanStepExt & step, CardinalityContext & card_context)
{
    PlanNodeStatisticsPtr stats = TableScanEstimator::estimate(card_context.context, step);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitReadNothingStep(const ReadNothingStep &, CardinalityContext &)
{
    return std::make_shared<PlanNodeStatistics>();
}

PlanNodeStatisticsPtr CardinalityVisitor::visitReadStorageRowCountStepExt(const ReadStorageRowCountStepExt &, CardinalityContext &)
{
    return std::make_shared<PlanNodeStatistics>(1);
}

PlanNodeStatisticsPtr CardinalityVisitor::visitValuesStepExt(const ValuesStepExt & step, CardinalityContext &)
{
    return std::make_shared<PlanNodeStatistics>(step.getRows());
}

PlanNodeStatisticsPtr CardinalityVisitor::visitLimitStepExt(const LimitStepExt & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = LimitEstimator::estimate(child_stats, step);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitLimitByStep(const LimitByStep & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = LimitEstimator::estimate(child_stats, step);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitFinishSortingStepExt(const FinishSortingStepExt & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = SortingEstimator::estimate(child_stats, step);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitSortingStepExt(const SortingStepExt & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = SortingEstimator::estimate(child_stats, step);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitMergeSortingStepExt(const MergeSortingStepExt & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = SortingEstimator::estimate(child_stats, step);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitPartialSortingStepExt(const PartialSortingStepExt & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = SortingEstimator::estimate(child_stats, step);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitMergingSortedStepExt(const MergingSortedStepExt & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = SortingEstimator::estimate(child_stats, step);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitPartitionTopNStepExt(const PartitionTopNStepExt &, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;
}

//PlanNodeStatisticsPtr CardinalityVisitor::visitMaterializingStep(const MaterializingStep &, CardinalityContext &)
//{
//    throw Exception("MaterializingNode current not support", ErrorCodes::NOT_IMPLEMENTED);
//}
//
//PlanNodeStatisticsPtr CardinalityVisitor::visitDecompressionStep(const DecompressionStep &, CardinalityContext &)
//{
//    throw Exception("DecompressionNode current not support", ErrorCodes::NOT_IMPLEMENTED);
//}

PlanNodeStatisticsPtr CardinalityVisitor::visitDistinctStepExt(const DistinctStepExt & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = AggregateEstimator::estimate(child_stats, step, context.context);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitExtremesStep(const ExtremesStep &, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;
}

//PlanNodeStatisticsPtr CardinalityVisitor::visitFinalSamplingStep(const FinalSamplingStep &, CardinalityContext &)
//{
//    throw Exception("FinalSamplingNode current not support", ErrorCodes::NOT_IMPLEMENTED);
//}

PlanNodeStatisticsPtr CardinalityVisitor::visitApplyStepExt(const ApplyStepExt &, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitCTERefStepExt(const CTERefStepExt & step, CardinalityContext & context)
{
    auto cte_def = context.cte_info.getCTEDef(step.getId());
    auto result = CardinalityEstimator::estimate(*cte_def, context.cte_info, context.context);

    if (!result)
        return nullptr;

    auto & stats = result.value();
    std::unordered_map<String, SymbolStatisticsPtr> calculated_symbol_statistics;
    for (const auto & item : step.getOutputColumns())
        calculated_symbol_statistics[item.first] = stats->getSymbolStatistics(item.second);
    return std::make_shared<PlanNodeStatistics>(stats->getRowCount(), std::move(calculated_symbol_statistics));
}

PlanNodeStatisticsPtr CardinalityVisitor::visitEnforceSingleRowStepExt(const EnforceSingleRowStepExt & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = EnforceSingleRowEstimator::estimate(child_stats, step);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitAssignUniqueIdStepExt(const AssignUniqueIdStepExt & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = AssignUniqueIdEstimator::estimate(child_stats, step);
    return stats;
}


PlanNodeStatisticsPtr CardinalityVisitor::visitExplainAnalyzeStepExt(const ExplainAnalyzeStepExt &, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitTopNFilteringStepExt(const TopNFilteringStepExt &, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitMultiJoinStepExt(const MultiJoinStepExt & , CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitFillingStep(const FillingStep & , CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitIntermediateResultCacheStepExt(const IntermediateResultCacheStepExt &, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;
}

PlanNodeStatisticsPtr PlanCardinalityVisitor::visitPlanNode(PlanNodeBase & node, CardinalityContext & context)
{
    static CardinalityVisitor visitor;

    std::vector<PlanNodeStatisticsPtr> children_stats;
    bool simple_children = true;
    bool is_table_scan = getQueryPlanStepType(node.getStep()) == QueryPlanStepType::TableScanStepExt;
    std::vector<bool> children_are_table_scan;

    for (auto & child : node.getChildren())
    {
        CardinalityContext children_context{.context = context.context, .cte_info = context.cte_info, .children_stats = {}};
        children_stats.emplace_back(VisitorUtil::accept(*child, *this, children_context));

        simple_children &= children_context.simple_children;
        children_are_table_scan.emplace_back(children_context.is_table_scan);
        if (getQueryPlanStepType(node.getStep()) == QueryPlanStepType::ProjectionStepExt)
        {
            is_table_scan = children_context.is_table_scan;
        }
        
        // ignore runtime filter 
        if (getQueryPlanStepType(node.getStep()) == QueryPlanStepType::FilterStepExt)
        {
            const FilterStepExt & step = dynamic_cast<FilterStepExt &>(*node.getStep());
            bool all_runtime_filters = true;
            for (auto & conjunct : PredicateUtils::extractConjuncts(step.getFilter()))
            {
                // $runtimeFilter(1265, `ws_sold_date_sk`, 0.8028399781540142)
                if (conjunct->getColumnName().find(InternalFunctionRuntimeFilter::name) == std::string::npos)
                {
                    all_runtime_filters = false;
                }
            }
            if (all_runtime_filters) 
            {
                is_table_scan = children_context.is_table_scan;
            }
        }
    }

    simple_children &= getQueryPlanStepType(node.getStep()) != QueryPlanStepType::Join;

    context.is_table_scan = is_table_scan;
    context.simple_children = simple_children;

    if (node.getStatistics().isDerived() && !context.re_estimate)
        return node.getStatistics().value_or(nullptr);

    CardinalityContext cardinality_context{
        .context = context.context,
        .cte_info = context.cte_info,
        .children_stats = std::move(children_stats),
        .simple_children = simple_children,
        .children_are_table_scan = children_are_table_scan,
        .re_estimate = context.re_estimate};
    auto step = node.getStep();
    auto stats = VisitorUtil::accept(step, visitor, cardinality_context);
    node.setStatistics(stats ? std::make_optional(stats) : std::nullopt);
    return stats;
}

PlanNodeStatisticsPtr PlanCardinalityVisitor::visitCTERefStepExtNode(CTERefStepExtNode & node, CardinalityContext & context)
{
    const auto * step = dynamic_cast<const CTERefStepExt *>(node.getStep().get());
    cte_helper.accept(step->getId(), *this, context);

    if (node.getStatistics().isDerived())
        return node.getStatistics().value_or(nullptr);
    auto result = cte_helper.getCTEInfo().getCTEDef(step->getId())->getStatistics();
    if (!result)
        return nullptr;

    const auto & cte_ref_stats = result.value();
    std::unordered_map<String, SymbolStatisticsPtr> calculated_symbol_statistics;
    for (const auto & item : step->getOutputColumns())
        calculated_symbol_statistics[item.first] = cte_ref_stats->getSymbolStatistics(item.second);
    auto stats = std::make_shared<PlanNodeStatistics>(cte_ref_stats->getRowCount(), calculated_symbol_statistics);
    node.setStatistics(stats ? std::make_optional(stats) : std::nullopt);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitTotalsHavingStepExt(const TotalsHavingStepExt & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr stats = context.children_stats[0];
    if (const auto & having = step.getHavingFilter())
        stats = FilterEstimator::estimate(
            stats, having, step.getInputStreams()[0].header.getNamesToTypes(), context.context, context.simple_children);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitExpandStepExt(const ExpandStepExt & , CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;
}

}
