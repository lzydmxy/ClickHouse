
#pragma once

#include <Query/Optimizer/Cascades/GroupExpression.h>
#include <Query/Optimizer/Rule/Rule.h>
#include <QueryPlan/AnyStep.h>

namespace DB
{
class CascadesContext;

/**
 * Magic Set create special partial structure to filter data early.
 * While these options may result in more computation inside the view,
 * they could be cheaper overall (because the Partial Result or
 * the Filter table is cheaper to compute).
 *
 * see more
 * - Cost-Based Optimization for Magic: Algebra and Implementation
 * - Sideways Information Passing for Push-Style Query Processing
 */
class MagicSetRule : public Rule
{
public:
    RuleType getType() const override = 0;
    ConstRefPatternPtr getPattern() const override = 0;
    const std::vector<RuleType> & blockRules() const override;
    bool isEnabled(ContextPtr context) const override { return context->getSettingsRef().enable_magic_set; }

    static double getFilterFactor(
        const PlanNodeStatisticsPtr & source_statistics,
        const PlanNodeStatisticsPtr & filter_statistics,
        const Names & source_names,
        const Names & filter_names);

    /*
     * Build Magic Set as special join: X Semi join Y.
     * Y is small filter source, X is big target.
     * <pre>
     * - Semi Join (magic set filter join)
     *     - X
     *     - Projection (prune column or reallocate symbol)
     *         - Y
     * </pre>
     */
    static PlanNodePtr buildMagicSetAsSemiJoin(
        const PlanNodePtr & source,
        PlanNodePtr filter_source,
        const Names & source_names,
        const Names & filter_names,
        CascadesContext & context);

    static void recordCTERefIntoGroup(PlanNodePtr plan_node, CascadesContext & context);
};

/**
 * Transforms:
 * <pre>
 * - Join or Right Join
 *     - Aggregating
 *         - X (large source as target )
 *     - Y (small source as filter)
 * </pre>
 * Into:
 * <pre>
 * - Join or Right Join
 *     - Aggregating
 *         - Semi Join (magic set filter join)
 *             - X
 *             - Projection (prune column or reallocate symbol)
 *                 - Y
 *     - Y
 * </pre>
 */
class MagicSetForAggregation : public MagicSetRule
{
public:
    RuleType getType() const override { return RuleType::MAGIC_SET_FOR_AGGREGATION; }
    String getName() const override { return "MAGIC_SET_FOR_AGGREGATION"; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

/**
 * Similar with MagicSetForAggregation.
 *
 * Transforms:
 * <pre>
 * - Join or Right Join
 *     - Projection
 *         - Aggregating
 *             - X (large source as target )
 *     - Y (small source as filter)
 * </pre>
 * Into:
 * <pre>
 * - Join or Right Join
 *     - Projection
 *         - Aggregating
 *             - Semi Join (magic set filter join)
 *                 - X
 *                 - Projection
 *                     - Y
 *     - Y
 * </pre>
 */
class MagicSetForProjectionAggregation : public MagicSetRule
{
public:
    RuleType getType() const override { return RuleType::MAGIC_SET_FOR_PROJECTION_AGGREGATION; }
    String getName() const override { return "MAGIC_SET_FOR_PROJECTION_AGGREGATION"; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
