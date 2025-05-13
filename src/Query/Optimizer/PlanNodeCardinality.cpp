#include <Query/Optimizer/PlanNodeCardinality.h>

#include <Query/Optimizer/ExpressionDeterminism.h>
#include <Query/Processors/QueryPlan/AggregatingStepExt.h>

namespace DB
{

class PlanNodeCardinality::Visitor : public PlanNodeVisitor<PlanNodeCardinality::Range, Void>
{
public:
    Range visitPlanNode(PlanNodeBase &, Void &) override { return Range{0, std::numeric_limits<size_t>::max()}; }

    static Range applyLimit(const Range & source, size_t limit)
    {
        limit = std::min(source.upper_bound, limit);
        size_t lower = std::min(limit, source.lower_bound);
        return Range{lower, limit};
    }

    static Range applyOffset(const Range & source, size_t offset)
    {
        return Range{
            std::max(source.lower_bound - offset, static_cast<size_t>(0)), std::max(source.upper_bound - offset, static_cast<size_t>(0))};
    }

    Range visitLimitStepExtNode(LimitStepExtNode & node, Void & context) override
    {
        auto source_range = VisitorUtil::accept(node.getChildren()[0], *this, context);
        const auto * step = dynamic_cast<const LimitStepExt *>(node.getStep().get());
        return step->hasPreparedParam() ? source_range
                                        : applyLimit(applyOffset(source_range, step->getOffset()), step->getLimit());
    }

    Range visitProjectionStepExtNode(ProjectionStepExtNode & node, Void & context) override
    {
        // todo: arrayJoin
        return VisitorUtil::accept(node.getChildren()[0], *this, context);
    }

    Range visitUnionStepExtNode(UnionStepExtNode & node, Void & context) override
    {
        if (node.getChildren().size() == 1)
            return PlanNodeVisitor::visitUnionStepExtNode(node, context);
        else
            return Range{0, std::numeric_limits<size_t>::max()};
    }

    Range visitExchangeStepExtNode(ExchangeStepExtNode & node, Void & context) override
    {
        return VisitorUtil::accept(node.getChildren()[0], *this, context);
    }

    Range visitFilterStepExtNode(FilterStepExtNode & node, Void & context) override
    {
        auto source_range = VisitorUtil::accept(node.getChildren()[0], *this, context);
        return Range{0, source_range.upper_bound};
    }

    Range visitEnforceSingleRowStepExtNode(EnforceSingleRowStepExtNode &, Void &) override { return Range{1, 1}; }

    Range visitValuesStepExtNode(ValuesStepExtNode & node, Void &) override
    {
        const auto * step = dynamic_cast<const ValuesStepExt *>(node.getStep().get());
        return Range{step->getRows(), step->getRows()};
    }

    Range visitAggregatingStepExtNode(AggregatingStepExtNode & node, Void & context) override
    {
        const auto * step = dynamic_cast<const AggregatingStepExt *>(node.getStep().get());
        if (step->getKeys().empty())
            return Range{1, 1};

        auto source_range = VisitorUtil::accept(node.getChildren()[0], *this, context);
        // hasDefaultOutput ? 1 : 0
        return Range{std::max(static_cast<size_t>(0), source_range.lower_bound), std::max(static_cast<size_t>(1), source_range.upper_bound)};
    }

    Range visitWindowStepNode(WindowStepNode & node, Void & context) override { return VisitorUtil::accept(node.getChildren()[0], *this, context); }

    Range visitDistinctStepExtNode(DistinctStepExtNode & node, Void & context) override
    {
        auto source_range = VisitorUtil::accept(node.getChildren()[0], *this, context);
        const auto *step = dynamic_cast<const DistinctStep *>(node.getStep().get());
        auto limit_hint = step->getLimitHint();
        if (limit_hint != 0)
            return Range{std::min(static_cast<size_t>(1), source_range.lower_bound), std::min(limit_hint, source_range.upper_bound)};
        return Range{std::min(static_cast<size_t>(1), source_range.lower_bound), source_range.upper_bound};
    }
};

PlanNodeCardinality::Range PlanNodeCardinality::extractCardinality(PlanNodeBase & node)
{
    PlanNodeCardinality::Visitor visitor;
    Void context{};
    return VisitorUtil::accept(node, visitor, context);
}

}
