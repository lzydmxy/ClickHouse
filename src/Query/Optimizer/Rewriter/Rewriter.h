#pragma once

#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Query/Processors/QueryPlan/PlanNode.h>

namespace DB
{
class Rewriter;

using RewriterPtr = std::shared_ptr<Rewriter>;
using Rewriters = std::vector<RewriterPtr>;
class Rewriter : public std::enable_shared_from_this<Rewriter>
{
public:
    virtual ~Rewriter() = default;
    virtual String name() const = 0;

    void rewritePlan(QueryPlanExt & plan, ContextMutablePtr context) const;

    RewriterPtr afterRules(Rewriters rewriters)
    {
        after_rules.insert(after_rules.end(), rewriters.begin(), rewriters.end());
        return shared_from_this();
    }

private:
    virtual bool rewrite(QueryPlanExt & plan, ContextMutablePtr context) const = 0;
    // every rewriter must set a setting to enable/disable it.
    virtual bool isEnabled(ContextMutablePtr context) const = 0;

    LoggerPtr log = getLogger("Rewriter");

    Rewriters after_rules;
};
}
