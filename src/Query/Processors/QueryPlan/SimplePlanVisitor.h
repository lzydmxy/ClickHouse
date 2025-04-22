#pragma once

#include <Query/Processors/QueryPlan/CTEVisitHelper.h>
#include <Query/Processors/QueryPlan/PlanVisitor.h>

namespace DB
{
template <typename C>
class SimplePlanVisitor : public PlanNodeVisitor<Void, C>
{
public:
    explicit SimplePlanVisitor(CTEInfo & cte_info) : cte_helper(cte_info) { }

    Void visitPlanNode(PlanNodeBase & node, C & c) override
    {
        for (const auto & child : node.getChildren())
            VisitorUtil::accept(*child, *this, c);
        return Void{};
    }

    Void visitCTERefNode(CTERefNode & node, C & c) override
    {
        const auto *cte_step = dynamic_cast<const CTERefStep *>(node.getStep().get());
        auto cte_id = cte_step->getId();
        cte_helper.accept(cte_id, *this, c);
        return Void{};
    }

protected:
    SimpleCTEVisitHelper<void> cte_helper;
};
}
