#pragma once

#include <Query/Optimizer/Rule/Rule.h>
#include <Query/Processors/QueryPlan/Assignment.h>
#include <Query/Processors/QueryPlan/PlanNode.h>

namespace DB
{

/**
 * if distributed_aggregation_memory_efficient is true, 
 * and aggregate is suitable for two-level, and aggregate is split into pattern
 * like MergingAggregated - Exchange - (Partial)Aggregating,
 * we set distributed_aggregation_memory_efficient for theses step,
 * and set keep_order for exchagne.
 */
class OptimizeMemoryEfficientAggregation : public Rule
{
public:
    RuleType getType() const override { return RuleType::OPTIMIZE_MEMORY_EFFICIENT_AGGREGATION; }
    String getName() const override { return "OPTIMIZE_MEMORY_EFFICIENT_AGGREGATION"; }
    bool isEnabled(ContextPtr context) const override
    {
        return context->getOptimizerContext()->getSettingsRef().enable_optimize_aggregate_memory_efficient
            && context->getSettingsRef().distributed_aggregation_memory_efficient;
    }
    ConstRefPatternPtr getPattern() const override;
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;

    bool excludeIfTransformSuccess() const override
    {
        return true;
    }
    bool excludeIfTransformFailure() const override
    {
        return true;
    }

    static bool isConvertibleToTwoLevel(const AggregatorExt::Params & params);
};

}
