#pragma once

#include <Query/Processors/QueryPlan/PlanVisitor.h>
#include <Query/Processors/QueryPlan/SimplePlanVisitor.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Query/Processors/QueryPlan/PlanNode.h>

namespace DB
{
class PlanPattern
{
public:
    static bool isSimpleQuery(QueryPlanExt & plan);
    static bool hasCrossJoin(QueryPlanExt & plan);
    static bool hasOuterJoin(QueryPlanExt & plan);
    static size_t maxJoinSize(QueryPlanExt & plan, ContextMutablePtr & context);
    static std::set<QueryPlanStepType> extractStepTypes(QueryPlanExt & plan);
};

class SimpleQueryPlanPatternVisitor : public SimplePlanVisitor<Void>
{
public:
    explicit SimpleQueryPlanPatternVisitor(CTEInfo & cte_info) : SimplePlanVisitor(cte_info) { }

    bool isSimpleQuery() const { return simple_query; }

    Void visitJoinStepExtNode(JoinStepExtNode &, Void &) override;
    Void visitApplyStepExtNode(ApplyStepExtNode &, Void &) override;
    Void visitIntersectOrExceptStepNode(IntersectOrExceptStepNode &, Void &) override;
    Void visitCTERefStepExtNode(CTERefStepExtNode &, Void &) override;

private:
    bool simple_query = true;
};

class CrossJoinPlanPatternVisitor : public SimplePlanVisitor<Void>
{
public:
    explicit CrossJoinPlanPatternVisitor(CTEInfo & cte_info) : SimplePlanVisitor(cte_info) { }

    bool hasCrossJoin() const { return has_cross_join; }

    Void visitJoinStepExtNode(JoinStepExtNode &, Void &) override;

private:
    bool has_cross_join = false;
};

class OuterJoinPlanPatternVisitor : public SimplePlanVisitor<Void>
{
public:
    explicit OuterJoinPlanPatternVisitor(CTEInfo & cte_info) : SimplePlanVisitor(cte_info) { }

    bool hasOuterJoin() const { return has_outer_join; }

    Void visitJoinStepExtNode(JoinStepExtNode &, Void &) override;

private:
    bool has_outer_join = false;
};

class GetMaxJoinSizeVisitor : public PlanNodeVisitor<size_t, Void>
{
public:
    explicit GetMaxJoinSizeVisitor(ContextMutablePtr context_, CTEInfo & cte_info) : context(context_), cte_helper(cte_info) { }
    size_t visitJoinStepExtNode(JoinStepExtNode &, Void &) override;
    size_t visitPlanNode(PlanNodeBase & node, Void & c) override
    {
        for (const auto & child : node.getChildren())
            VisitorUtil::accept(*child, *this, c);
        return 1;
    }
    size_t visitCTERefStepExtNode(CTERefStepExtNode & node, Void & c) override
    {
        const auto * cte_step = dynamic_cast<const CTERefStepExt *>(node.getStep().get());
        auto cte_id = cte_step->getId();
        cte_helper.accept(cte_id, *this, c);
        return 1;
    }

    size_t getMaxSize() const { return max_size; }

private:
    ContextMutablePtr context;
    SimpleCTEVisitHelper<void> cte_helper;
    size_t max_size = 0;
};

class ExtractTypesVisitor : public SimplePlanVisitor<Void>
{
public:
    explicit ExtractTypesVisitor(CTEInfo & cte_info) : SimplePlanVisitor(cte_info) { }

    Void visitPlanNode(PlanNodeBase & node, Void & c) override
    {
        types.insert(node.getType());
        for (const auto & child : node.getChildren()) VisitorUtil::accept(*child, *this, c);
        return c;
    }

    const std::set<QueryPlanStepType> & getTypes() const { return types; }

private:
    std::set<QueryPlanStepType> types;
};

}
