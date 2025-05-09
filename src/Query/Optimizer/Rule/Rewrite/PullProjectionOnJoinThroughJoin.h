#pragma once

#include <Query/Optimizer/Rule/Rule.h>

namespace DB
{

    /**
     * Pull projection for join graph rewrite.
     *
     * Transforms:
     * <pre>
     * - Inner Join / Left Join X
     *     - Projection
     *         - Join Y
     *     - Z
     * </pre>
     * Into:
     * <pre>
     * - Projection
     *     - Inner Join / Left Join X
     *         - Join Y
     *         - Z
     * </pre>
     */
    class PullProjectionOnJoinThroughJoin : public Rule
    {
    public:
        RuleType getType() const override { return RuleType::PULL_PROJECTION_ON_JOIN_THROUGH_JOIN; }
        String getName() const override { return "PULL_PROJECTION_ON_JOIN_THROUGH_JOIN"; }
        bool isEnabled(ContextPtr context) const override {return context->getOptimizerContext()->getSettingsRef().enable_pull_projection_on_join_through_join; }
        ConstRefPatternPtr getPattern() const override;

        TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
    };

}

