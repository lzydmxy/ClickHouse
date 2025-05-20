#pragma once

#include <Interpreters/Context_fwd.h>
#include <Query/Optimizer/Rewriter/Rewriter.h>
#include <Query/Processors/QueryPlan/QueryPlanExt.h>

namespace DB
{

class AddCache : public Rewriter
{
public:
    String name() const override { return "AddCache"; }

private:
    bool isEnabled(ContextMutablePtr context) const override { return context->getOptimizerContext()->getSettingsRef().enable_intermediate_result_cache; }
    bool rewrite(QueryPlanExt & plan, ContextMutablePtr context) const override;
};

}
