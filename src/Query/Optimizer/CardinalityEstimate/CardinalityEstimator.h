

#pragma once

#include <Query/Analyzer/TypeAnalyzer.h>
#include <Interpreters/Context.h>
#include <Query/Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Query/Processors/QueryPlan/PlanVisitor.h>
#include <Query/Processors/QueryPlan/CTEVisitHelper.h>
#include <Query/Optimizer/DataDependency/InclusionDependency.h>

namespace DB
{
class CardinalityEstimator
{
public:
    static std::optional<PlanNodeStatisticsPtr> estimate(
        QueryPlanStepPtr & step,
        CTEInfo & cte_info,
        std::vector<PlanNodeStatisticsPtr> children_stats,
        ContextMutablePtr context,
        bool simple_children,
        std::vector<bool> is_table_scan,
        std::vector<double> children_filter_selectivity,
        const InclusionDependency & inclusion_dependency = {});

    static std::optional<PlanNodeStatisticsPtr> estimate(
        PlanNodeBase & node,
        CTEInfo & cte_info,
        ContextMutablePtr context,
        bool recursive = false, 
        bool re_estimate = false);

    static void estimate(QueryPlanExt & plan, ContextMutablePtr context, bool re_estimate = false);
};

struct CardinalityContext
{
    ContextMutablePtr context;
    CTEInfo & cte_info;
    std::vector<PlanNodeStatisticsPtr> children_stats;
    bool simple_children = false;
    std::vector<bool> children_are_table_scan = {};
    bool is_table_scan = false;
    bool re_estimate = false;
    std::vector<double> children_filter_selectivity = {};
    InclusionDependency inclusion_dependency = {};
};

class CardinalityVisitor : public StepVisitor<PlanNodeStatisticsPtr, CardinalityContext>
{
public:
    PlanNodeStatisticsPtr visitStep(const IQueryPlanStep &, CardinalityContext &) override;

#define VISITOR_DEF(TYPE) PlanNodeStatisticsPtr visit##TYPE(const TYPE &, CardinalityContext &) override;
    APPLY_PROTOBUF_STEP_TYPES(VISITOR_DEF)
#undef VISITOR_DEF
};

class PlanCardinalityVisitor : public PlanNodeVisitor<PlanNodeStatisticsPtr, CardinalityContext>
{
public:
    explicit PlanCardinalityVisitor(CTEInfo & cte_info) : cte_helper(cte_info) { }

    PlanNodeStatisticsPtr visitPlanNode(PlanNodeBase &, CardinalityContext &) override;
    PlanNodeStatisticsPtr visitCTERefStepExtNode(CTERefStepExtNode & node, CardinalityContext & context) override;
private:
    SimpleCTEVisitHelper<void> cte_helper;
};

}
