#pragma once

#include <Common/Logger.h>
#include <Interpreters/Context.h>
#include <Query/Optimizer/EqualityInference.h>
#include <Query/Optimizer/Rewriter/Rewriter.h>
#include <Query/Processors/QueryPlan/PlanVisitor.h>
#include <Query/Processors/QueryPlan/CTEInfo.h>

#include <Query/Processors/QueryPlan/PlanNode.h>

namespace DB
{
class PredicatePushdown : public Rewriter
{
public:
    explicit PredicatePushdown(bool pushdown_filter_into_cte_ = false, bool simplify_common_filter_ = false)
        : pushdown_filter_into_cte(pushdown_filter_into_cte_), simplify_common_filter(simplify_common_filter_)
    {
    }
    String name() const override { return "PredicatePushdown"; }

private:
    bool rewrite(QueryPlanExt & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override { return context->getOptimizerContext()->getSettingsRef().enable_predicate_pushdown_rewrite; }
    const bool pushdown_filter_into_cte;
    const bool simplify_common_filter;
};

struct PredicateContext
{
    ConstASTPtr predicate;
    ConstASTPtr extra_predicate_for_simplify_outer_join;
    ContextMutablePtr context;
};

struct InnerJoinResult;
struct OuterJoinResult;

class PredicateVisitor : public PlanNodeVisitor<PlanNodePtr, PredicateContext>
{
public:
    PredicateVisitor(
        bool pushdown_filter_into_cte_,
        bool simplify_common_filter_,
        ContextMutablePtr context_,
        CTEInfo & cte_info_,
        const std::unordered_map<CTEId, UInt64> & cte_reference_counts_)
        : pushdown_filter_into_cte(pushdown_filter_into_cte_)
        , simplify_common_filter(simplify_common_filter_)
        , context(context_)
        , cte_info(cte_info_)
        , cte_reference_counts(cte_reference_counts_)
    {
    }

    PlanNodePtr visitPlanNode(PlanNodeBase &, PredicateContext &) override;
    PlanNodePtr visitProjectionStepExtNode(ProjectionStepExtNode &, PredicateContext &) override;
    PlanNodePtr visitFilterStepExtNode(FilterStepExtNode &, PredicateContext &) override;
    PlanNodePtr visitAggregatingStepExtNode(AggregatingStepExtNode &, PredicateContext &) override;
    PlanNodePtr visitMarkDistinctStepExtNode(MarkDistinctStepExtNode & node, PredicateContext & predicate_context) override;
    PlanNodePtr visitJoinStepExtNode(JoinStepExtNode &, PredicateContext &) override;
    PlanNodePtr visitArrayJoinStepNode(ArrayJoinStepNode &, PredicateContext &) override;
    PlanNodePtr visitExchangeStepExtNode(ExchangeStepExtNode & node, PredicateContext & predicate_context) override;
    PlanNodePtr visitWindowStepNode(WindowStepNode &, PredicateContext &) override;
    PlanNodePtr visitMergeSortingStepExtNode(MergeSortingStepExtNode &, PredicateContext &) override;
    // todo: lizhuoyu5 need PartialSortingStep
    // PlanNodePtr visitPartialSortingNode(PartialSortingStepNode &, PredicateContext &) override;
    PlanNodePtr visitSortingStepNode(SortingStepNode &, PredicateContext &) override;
    PlanNodePtr visitUnionStepExtNode(UnionStepExtNode &, PredicateContext &) override;
    PlanNodePtr visitDistinctStepExtNode(DistinctStepExtNode &, PredicateContext &) override;
    PlanNodePtr visitAssignUniqueIdStepExtNode(AssignUniqueIdStepExtNode &, PredicateContext &) override;
    PlanNodePtr visitCTERefStepExtNode(CTERefStepExtNode & node, PredicateContext & context) override;

private:
    const bool pushdown_filter_into_cte;
    const bool simplify_common_filter;
    ContextMutablePtr context;
    CTEInfo & cte_info;
    const std::unordered_map<CTEId, UInt64> & cte_reference_counts;
    std::unordered_map<CTEId, std::vector<std::pair<const CTERefStepExt *, ConstASTPtr>>> cte_predicates{};
    LoggerPtr logger = getLogger("PredicateVisitor");

    PlanNodePtr process(PlanNodeBase &, PredicateContext &);
    PlanNodePtr processChild(PlanNodeBase &, PredicateContext &);
    InnerJoinResult processInnerJoin(
        ConstASTPtr & inherited_predicate,
        ConstASTPtr & left_predicate,
        ConstASTPtr & right_predicate,
        ConstASTPtr & join_predicate,
        std::set<String> & left_symbols,
        std::set<String> & right_symbols);
    OuterJoinResult processOuterJoin(
        ConstASTPtr & inherited_predicate,
        ConstASTPtr & outer_predicate,
        ConstASTPtr & inner_predicate,
        ConstASTPtr & join_predicate,
        std::set<String> & outer_symbols,
        std::set<String> & inner_symbols);

    // utils of outer join to inner join
    static void tryNormalizeOuterToInnerJoin(JoinStepExtNode & node, const ConstASTPtr & inherited_predicate, ContextMutablePtr context);
    static bool canConvertOuterToInner(
        const std::unordered_map<String, Field> & inner_symbols_for_outer_join,
        const ConstASTPtr & inherited_predicate,
        ContextMutablePtr context,
        const NameToType & column_types);
    static JoinKind useInnerForLeftSide(JoinKind kind);
    static JoinKind useInnerForRightSide(JoinKind kind);
    static bool isRegularJoin(const JoinStepExt & step);
};

struct InnerJoinResult
{
    ASTPtr left_predicate;
    ASTPtr right_predicate;
    ASTPtr join_predicate;
    ASTPtr post_join_predicate;
};

struct OuterJoinResult
{
    ASTPtr outer_predicate;
    ASTPtr inner_predicate;
    ASTPtr join_predicate;
    ASTPtr post_join_predicate;
};

/**
 * Computes the effective predicate at the top of the specified PlanNode
 *
 * Note: non-deterministic predicates cannot be pulled up (so they will be ignored)
 */
class EffectivePredicateExtractor
{
public:
    static ASTPtr extract(PlanNodePtr & node, ContextMutablePtr & context);
    static ASTPtr extract(PlanNodeBase & node, ContextMutablePtr & context);
};

class EffectivePredicateVisitor : public PlanNodeVisitor<ASTPtr, ContextMutablePtr>
{
protected:
    ASTPtr visitPlanNode(PlanNodeBase & node, ContextMutablePtr & context) override;

public:
    ASTPtr visitLimitStepExtNode(LimitStepExtNode &, ContextMutablePtr &) override;
    ASTPtr visitEnforceSingleRowStepExtNode(EnforceSingleRowStepExtNode &, ContextMutablePtr &) override;
    ASTPtr visitProjectionStepExtNode(ProjectionStepExtNode &, ContextMutablePtr &) override;
    ASTPtr visitFilterStepExtNode(FilterStepExtNode &, ContextMutablePtr &) override;
    ASTPtr visitAggregatingStepExtNode(AggregatingStepExtNode &, ContextMutablePtr &) override;
    ASTPtr visitJoinStepExtNode(JoinStepExtNode &, ContextMutablePtr &) override;
    ASTPtr visitExchangeStepExtNode(ExchangeStepExtNode &, ContextMutablePtr &) override;
    ASTPtr visitWindowStepNode(WindowStepNode &, ContextMutablePtr &) override;
    ASTPtr visitMergeSortingStepExtNode(MergeSortingStepExtNode &, ContextMutablePtr &) override;
    ASTPtr visitUnionStepExtNode(UnionStepExtNode &, ContextMutablePtr &) override;
    ASTPtr visitTableScanStepExtNode(TableScanStepExtNode &, ContextMutablePtr &) override;
    ASTPtr visitDistinctStepExtNode(DistinctStepExtNode &, ContextMutablePtr &) override;
    ASTPtr visitAssignUniqueIdStepExtNode(AssignUniqueIdStepExtNode &, ContextMutablePtr &) override;
    ASTPtr visitCTERefStepExtNode(CTERefStepExtNode & node, ContextMutablePtr & context) override;

    explicit EffectivePredicateVisitor() = default;

private:
    ASTPtr process(PlanNodeBase & node, ContextMutablePtr & context);
    static ASTPtr pullExpressionThroughSymbols(ASTPtr & expression, std::vector<String> symbols, ContextMutablePtr & context);
};

}
