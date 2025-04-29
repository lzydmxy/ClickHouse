#pragma once

#include <Query/Optimizer/Rewriter/Rewriter.h>
#include <Query/Processors/QueryPlan/CTEInfo.h>

namespace DB
{
/**
 * Find common plan node and rewrite with cte using signature.
 */
class ShareCommonPlanNode : public Rewriter
{
public:
    String name() const override
    {
        return "ShareCommonPlanNode";
    }

    static PlanNodePtr
    createCTERefNode(CTEId cte_id, const DataStream & output_stream, const DataStream & cte_output_stream, ContextMutablePtr context);

private:
    bool rewrite(QueryPlanExt & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override
    {
        return context->getOptimizerContext()->getSettingsRef().enable_share_common_plan_node && context->getOptimizerContext()->getSettingsRef().cte_mode != CTEMode::INLINED;
    }

    class Rewriter;
};
}
