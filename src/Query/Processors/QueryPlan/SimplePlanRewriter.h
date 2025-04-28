#pragma once

#include <Interpreters/Context.h>
#include <Query/Processors/QueryPlan/CTERefStepExt.h>
#include <Query/Processors/QueryPlan/CTEVisitHelper.h>
#include <Query/Processors/QueryPlan/PlanNode.h>
#include <Query/Processors/QueryPlan/PlanVisitor.h>

namespace DB
{
template <typename C>
class SimplePlanRewriter : public PlanNodeVisitor<PlanNodePtr, C>
{
public:
    SimplePlanRewriter(ContextMutablePtr context_, CTEInfo & cte_info) : context(std::move(context_)), cte_helper(cte_info) { }

    PlanNodePtr visitPlanNode(PlanNodeBase & node, C & c) override
    {
        if (node.getChildren().empty())
            return node.shared_from_this();
        PlanNodes children;
        for (const auto & item : node.getChildren())
        {
            PlanNodePtr child = VisitorUtil::accept(*item, *this, c);
            children.emplace_back(child);
        }

        node.replaceChildren(children);
        return node.shared_from_this();
    }

    PlanNodePtr visitCTERefStepExtNode(CTERefStepExtNode & node, C & c) override
    {
        auto cte_step = node.getStep();
        auto cte_id = cte_step->getId();
        auto cte_plan = cte_helper.acceptAndUpdate(cte_id, *this, c);
        return node.shared_from_this();
    }

protected:
    ContextMutablePtr context;
    SimpleCTEVisitHelper<PlanNodePtr> cte_helper;
};
}
