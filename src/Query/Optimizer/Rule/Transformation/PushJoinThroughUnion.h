
#pragma once
#include <Query/Optimizer/Rule/Rule.h>

namespace DB
{

/**
 * Transforms:
 * <pre>
 * - Join
 *     - Union
 *         - X
 *         - Y
 *         - ...
 *    - A
 * </pre>
 * Into:
 * <pre>
 * - Union
 *     - Join
 *         - X
 *         - A
 *     - Join
 *         - Y
 *         - A'
 *     - ...
 * </pre>
 */
class PushJoinThroughUnion : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_JOIN_THROUGH_UNION; }
    String getName() const override { return "PUSH_JOIN_THROUGH_UNION"; }
    bool isEnabled(ContextPtr context) const override {return context->getSettingsRef().enable_push_join_through_union; }
    ConstRefPatternPtr getPattern() const override;
    const std::vector<RuleType> & blockRules() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
