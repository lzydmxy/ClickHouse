#include <Query/Optimizer/Rewriter/Rewriter.h>
#include <Query/Processors/QueryPlan/PlanVisitor.h>
#include <Query/Processors/QueryPlan/SimplePlanRewriter.h>

#include <Query/Processors/QueryPlan/PlanNode.h>
#include <Query/Parsers/ASTVisitor.h>


namespace DB
{

class RemoveRedundantSort : public Rewriter
{
public:
    String name() const override { return "RemoveRedundantSort"; }

private:
    bool rewrite(QueryPlanExt & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override { return context->getOptimizerContext()->getSettingsRef().enable_redundant_sort_removal; }
};

struct RedundantSortContext
{
    ContextMutablePtr context;
    bool can_sort_be_removed = false;
};

class RedundantSortVisitor : public SimplePlanRewriter<RedundantSortContext>
{
public:
    explicit RedundantSortVisitor(ContextMutablePtr context_, CTEInfo & cte_info_)
        : SimplePlanRewriter(context_, cte_info_)
    {
    }

    static bool isStateful(ConstASTPtr expression, ContextMutablePtr context);
    static bool isOrderDependentAggregateFunction(const String & aggname);
    const static std::unordered_set<String> order_dependent_agg;

private:
    PlanNodePtr visitPlanNode(PlanNodeBase & node, RedundantSortContext & sort_context) override;
    PlanNodePtr visitProjectionStepExtNode(ProjectionStepExtNode & node, RedundantSortContext & sort_context) override;
    PlanNodePtr visitAggregatingStepExtNode(AggregatingStepExtNode & node, RedundantSortContext & sort_context) override;
    PlanNodePtr visitJoinStepExtNode(JoinStepExtNode & node, RedundantSortContext & sort_context) override;
    PlanNodePtr visitUnionStepExtNode(UnionStepExtNode & node, RedundantSortContext & sort_context) override;
    PlanNodePtr visitIntersectOrExceptStepNode(IntersectOrExceptStepNode & node, RedundantSortContext & sort_context) override;
    PlanNodePtr visitSortingStepExtNode(SortingStepExtNode & node, RedundantSortContext & sort_context) override;
    PlanNodePtr visitCTERefStepExtNode(CTERefStepExtNode & node, RedundantSortContext & sort_context) override;

    PlanNodePtr processChildren(PlanNodeBase & node, RedundantSortContext & sort_context);
    PlanNodePtr resetChild(PlanNodeBase & node, PlanNodes & children, RedundantSortContext & sort_context);

    std::unordered_map<CTEId, RedundantSortContext> cte_require_context{};
};

class StatefulVisitor : public ConstASTVisitor<void, ContextMutablePtr>
{
public:
    void visitNode(const ConstASTPtr & node, ContextMutablePtr & context) override;
    void visitASTFunction(const ConstASTPtr & node, ContextMutablePtr & context) override;
    bool isStateful() const { return is_stateful; }

private:
    bool is_stateful = false;
};
}
