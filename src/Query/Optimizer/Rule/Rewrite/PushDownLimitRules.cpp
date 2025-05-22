#include <Core/SortDescription.h>

#include <Query/Optimizer/Rule/Rewrite/PushDownLimitRules.h>
#include <Query/Optimizer/PlanNodeCardinality.h>
#include <Query/Optimizer/Rule/Patterns.h>
#include <Query/Optimizer/Utils.h>

#include <Query/Processors/QueryPlan/JoinStepExt.h>
#include <Query/Processors/QueryPlan/LimitStepExt.h>
#include <Processors/QueryPlan/WindowStep.h>

namespace DB
{
static bool isLimitNeeded(const LimitStepExt & limit, const PlanNodePtr & node)
{
    auto range = PlanNodeCardinality::extractCardinality(*node);
    return !limit.hasPreparedParam() && range.upper_bound > limit.getLimit() + limit.getOffset();
}

ConstRefPatternPtr PushLimitIntoDistinct::getPattern() const
{
    static auto pattern = Patterns::limit().withSingle(Patterns::distinct()).result();
    return pattern;
}

TransformResult PushLimitIntoDistinct::transformImpl(PlanNodePtr node, const Captures &, RuleContext &)
{
    const auto * limit_step = dynamic_cast<const LimitStepExt *>(node->getStep().get());
    auto distinct = node->getChildren()[0];
    const auto * distinct_step = dynamic_cast<const DistinctStepExt *>(distinct->getStep().get());

    if (!isLimitNeeded(*limit_step, distinct))
        return {};

    // when limit 0, we skip this rule since another rule will delete the whole node
    auto limit_value = limit_step->getLimit();
    if (limit_value == 0)
        return {};

    auto new_distinct = PlanNodeBase::createPlanNode(
        distinct->getId(),
        std::make_shared<DistinctStepExt>(
            distinct_step->getInputStreams()[0],
            distinct_step->getSetSizeLimits(),
            limit_step->getLimit() + limit_step->getOffset(),
            distinct_step->getColumns(),
            distinct_step->preDistinct(),
            distinct_step->getOptimizeDistinctInOrder(),
            false),
        distinct->getChildren());
    node->replaceChildren({new_distinct});
    return TransformResult{node};
}

ConstRefPatternPtr PushLimitThroughProjection::getPattern() const
{
    static auto pattern = Patterns::limit().withSingle(Patterns::project()).result();
    return pattern;
}

TransformResult PushLimitThroughProjection::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_ctx)
{
    auto projection = node->getChildren()[0];
    const auto * projection_step = dynamic_cast<const ProjectionStepExt *>(projection->getStep().get());
    if (!projection_step || Utils::canChangeOutputRows(*projection_step, rule_ctx.context))
        return {};

    auto source = projection->getChildren()[0];
    const auto * limit_step = dynamic_cast<const LimitStepExt *>(node->getStep().get());
    auto new_limit = PlanNodeBase::createPlanNode(
        node->getId(),
        std::make_shared<LimitStepExt>(
            source->getStep()->getOutputStream(),
            limit_step->getLimit(),
            limit_step->getOffset(),
            limit_step->isAlwaysReadTillEnd(),
            limit_step->withTies(),
            limit_step->getSortDescription(),
            limit_step->isPartial()),
        {source});

    projection->replaceChildren({new_limit});
    projection->setStatistics(node->getStatistics());
    return TransformResult{projection, true};
}

ConstRefPatternPtr PushLimitThroughExtremesStep::getPattern() const
{
    static auto pattern = Patterns::limit().withSingle(Patterns::extremes()).result();
    return pattern;
}

TransformResult PushLimitThroughExtremesStep::transformImpl(PlanNodePtr node, const Captures &, RuleContext &)
{
    auto extremes = node->getChildren()[0];
    auto source = extremes->getChildren()[0];

    node->replaceChildren({source});
    extremes->replaceChildren({node->shared_from_this()});
    return TransformResult{extremes, true};
}

ConstRefPatternPtr PushLimitThroughUnion::getPattern() const
{
    static auto pattern = Patterns::limit().withSingle(Patterns::unionn()).result();
    return pattern;
}

TransformResult PushLimitThroughUnion::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto * limit_step = dynamic_cast<const LimitStepExt *>(node->getStep().get());
    auto unionn = node->getChildren()[0];
    bool should_apply = false;
    PlanNodes children;

    for (auto & child : unionn->getChildren())
    {
        if (isLimitNeeded(*limit_step, child))
        {
            children.emplace_back(PlanNodeBase::createPlanNode(
                context.context->getOptimizerContext()->nextNodeId(),
                std::make_shared<LimitStepExt>(
                    child->getStep()->getOutputStream(),
                    limit_step->getLimit() + limit_step->getOffset(),
                    size_t{0},
                    limit_step->isAlwaysReadTillEnd(),
                    limit_step->withTies(),
                    limit_step->getSortDescription(),
                    true),
                {child}));
            should_apply = true;
        }
        else
        {
            children.emplace_back(child);
        }
    }

    if (!should_apply)
        return {};

    unionn->replaceChildren(children);
    node->replaceChildren({unionn});
    return node;
}

ConstRefPatternPtr PushLimitThroughOuterJoin::getPattern() const
{
    static auto pattern = Patterns::limit()
        .withSingle(Patterns::join().matchingStep<JoinStepExt>([](const auto & join_step) { return join_step.isLeftOrRightOuterJoin(); }))
        .result();
    return pattern;
}

TransformResult PushLimitThroughOuterJoin::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    auto join = node->getChildren()[0];
    const auto * join_step = dynamic_cast<const JoinStepExt *>(join->getStep().get());
    const auto * limit_step = dynamic_cast<const LimitStepExt *>(node->getStep().get());
    auto left = join->getChildren()[0];
    auto right = join->getChildren()[1];

    if (join_step->isLeftOuterJoin() && isLimitNeeded(*limit_step, left))
    {
        left = PlanNodeBase::createPlanNode(
            context.context->getOptimizerContext()->nextNodeId(),
            std::make_shared<LimitStepExt>(
                left->getStep()->getOutputStream(),
                limit_step->getLimit() + limit_step->getOffset(),
                size_t{0},
                limit_step->isAlwaysReadTillEnd(),
                limit_step->withTies(),
                limit_step->getSortDescription(),
                true),
            {left});
        join->replaceChildren({left, right});
        return node;
    }
    else if (join_step->isRightOuterJoin() && isLimitNeeded(*limit_step, right))
    {
        right = PlanNodeBase::createPlanNode(
            context.context->getOptimizerContext()->nextNodeId(),
            std::make_shared<LimitStepExt>(
                right->getStep()->getOutputStream(),
                limit_step->getLimit() + limit_step->getOffset(),
                size_t{0},
                limit_step->isAlwaysReadTillEnd(),
                limit_step->withTies(),
                limit_step->getSortDescription(),
                true),
            {right});
        join->replaceChildren({left, right});
        return node;
    }

    return {};
}

ConstRefPatternPtr LimitZeroToReadNothing::getPattern() const
{
    static auto pattern = Patterns::limit()
        .matchingStep<LimitStepExt>([](const LimitStepExt & step) { return !step.hasPreparedParam() && step.getLimit() == 0; })
        .result();
    return pattern;
}

TransformResult LimitZeroToReadNothing::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto limit_node = dynamic_cast<LimitStepExtNode *>(node.get());
    if (!limit_node)
        return {};

    const auto & step = *limit_node->getStep();
    if (step.hasPreparedParam())
        return {};
    if (step.getLimit() == 0)
    {
        auto read_nothing_step = std::make_shared<ReadNothingStep>(step.getOutputStream().header);
        auto read_nothing_node = PlanNodeBase::createPlanNode(rule_context.context->getOptimizerContext()->nextNodeId(), std::move(read_nothing_step), {});
        return {read_nothing_node};
    }

    return {};
}

ConstRefPatternPtr PushdownLimitIntoWindow::getPattern() const
{
    static auto pattern = Patterns::limit().withSingle(Patterns::window().matchingStep<WindowStep>([](const WindowStep & window_step) {
        bool all_row_number = true;
        for (const auto & func : QueryPlanStepHelper::getWindowStepFunctions(window_step))
        {
            all_row_number &= func.aggregate_function->getName() == "row_number";
        }
        return all_row_number && !QueryPlanStepHelper::getWindowStepWindow(window_step).order_by.empty();
    })).result();
    return pattern;
}

TransformResult PushdownLimitIntoWindow::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    auto window = node->getChildren()[0];
    const auto * window_step = dynamic_cast<const WindowStep *>(window->getStep().get());
    const auto * limit_step = dynamic_cast<const LimitStepExt *>(node->getStep().get());
    auto source = window->getChildren()[0];

    if (limit_step->hasPreparedParam())
        return {};

    if (getQueryPlanStepType(*source->getStep()) == QueryPlanStepType::LimitStepExt || QueryPlanStepHelper::getWindowStepWindow(*window_step).order_by.empty())
    {
        return {};
    }

    auto new_sort = PlanNodeBase::createPlanNode(
        context.context->getOptimizerContext()->nextNodeId(),
        std::make_shared<SortingStepExt>(
            source->getStep()->getOutputStream(), QueryPlanStepHelper::getWindowStepWindow(*window_step).order_by, limit_step->getLimit(), SortingStepExt::Stage::FULL, SortDescription{}),
        {source});

    auto new_limit = PlanNodeBase::createPlanNode(
        context.context->getOptimizerContext()->nextNodeId(),
        std::make_shared<LimitStepExt>(
            source->getStep()->getOutputStream(),
            limit_step->getLimit(),
            limit_step->getOffset(),
            limit_step->isAlwaysReadTillEnd(),
            limit_step->withTies(),
            limit_step->getSortDescription(),
            limit_step->isPartial()),
        {new_sort});

    window->replaceChildren({new_limit});
    node->replaceChildren({window});
    return {node};
}

ConstRefPatternPtr PushLimitIntoSorting::getPattern() const
{
    static auto pattern = Patterns::limit()
        .matchingStep<LimitStepExt>([](const LimitStepExt & step) { return step.getLimit() != 0; })
        .withSingle(Patterns::sorting().matchingStep<SortingStepExt>(
            [](const auto & sorting_step) { return sorting_step.getLimit() == 0; }))
        .result();
    return pattern;
}

TransformResult PushLimitIntoSorting::transformImpl(PlanNodePtr node, const Captures &, RuleContext &)
{
    const auto *limit_step = dynamic_cast<const LimitStepExt *>(node->getStep().get());
    auto sorting = node->getChildren()[0];
    const auto *sorting_step = dynamic_cast<const SortingStepExt *>(sorting->getStep().get());

    // when limit 0, we skip this rule since another rule will delete the whole node
    auto limit_value = limit_step->getLimit();
    if (limit_value == 0)
        return {};

    if (!isLimitNeeded(*limit_step, sorting))
        return {};

    auto new_sorting = PlanNodeBase::createPlanNode(
        sorting->getId(),
        std::make_shared<SortingStepExt>(
            sorting_step->getInputStreams()[0],
            sorting_step->getSortDescription(),
            limit_step->getLimit() + limit_step->getOffset(),
            sorting_step->getStage(),
            sorting_step->getPrefixDescription()),
        sorting->getChildren());
    node->replaceChildren({new_sorting});
    return TransformResult{node};
}

ConstRefPatternPtr PushLimitThroughBuffer::getPattern() const
{
    static auto pattern = Patterns::limit().withSingle(Patterns::buffer()).result();
    return pattern;
}

TransformResult PushLimitThroughBuffer::transformImpl(PlanNodePtr node, const Captures &, RuleContext &)
{
    auto buffer = node->getChildren()[0];
    node->replaceChildren({buffer->getChildren()[0]});
    buffer->replaceChildren({node});
    return buffer;
}
}
