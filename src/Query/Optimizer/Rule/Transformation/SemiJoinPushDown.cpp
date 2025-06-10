#include <Query/Optimizer/Rule/Transformation/SemiJoinPushDown.h>

#include <Query/Optimizer/Cascades/CascadesOptimizer.h>
#include <Query/Optimizer/Rule/Patterns.h>
#include <Query/Optimizer/SymbolsExtractor.h>
#include <Query/Optimizer/Utils.h>
#include <Parsers/ASTIdentifier.h>
#include <Query/Common/NameToTypeExt.h>
#include <Query/Processors/QueryPlan/Assignment.h>
#include <Query/Processors/QueryPlan/PlanNode.h>
#include <Query/Processors/QueryPlan/ProjectionStepExt.h>
#include <Query/Planner//SymbolMapper.h>

#include <memory>
#include <unordered_set>

namespace DB
{
ConstRefPatternPtr SemiJoinPushDown::getPattern() const
{
    static auto pattern = Patterns::join()
        .matchingStep<JoinStepExt>([](const JoinStepExt & s) {
            return s.getKind() == JoinKind::Left
                && (s.getStrictness() == JoinStrictness::Semi || s.getStrictness() == JoinStrictness::Anti)
                && !s.isOrdered();
        }, "semijoin-matchingstep")
        .with(
            Patterns::join()
                .matchingStep<JoinStepExt>([](const JoinStepExt & s) { return !s.isOuterJoin() && !s.isOrdered(); })
                .with(Patterns::any(), Patterns::any()),
            Patterns::any())
        .result();
    return pattern;
}

TransformResult SemiJoinPushDown::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto * semi_join_node = dynamic_cast<JoinStepExtNode *>(node.get());
    auto * join_node = dynamic_cast<JoinStepExtNode *>(node->getChildren()[0].get());

    auto a_outer_columns = ToNameSet(join_node->getChildren()[0]->getCurrentDataStream().header.getNamesAndTypes());
    for (const auto & col : semi_join_node->getStep()->getLeftKeys())
    {
        if (!a_outer_columns.contains(col))
            return {};
    }

    DataStream output_stream;
    for (const auto & col : semi_join_node->getChildren()[0]->getChildren()[0]->getCurrentDataStream().header)
    {
        output_stream.header.insert(col);
    }
    for (const auto & col : semi_join_node->getChildren()[1]->getCurrentDataStream().header)
    {
        // any inner join cotains symbols from right
        if (node->getCurrentDataStream().header.has(col.name))
            output_stream.header.insert(col);
    }

    auto step = semi_join_node->getStep();
    DataStreams streams
        = {semi_join_node->getChildren()[0]->getStep()->getInputStreams()[1], semi_join_node->getChildren()[1]->getCurrentDataStream()};
    auto new_semi_step = std::make_shared<JoinStepExt>(
        streams,
        output_stream,
        step->getKind(),
        step->getStrictness(),
        step->getMaxStreams(),
        step->getKeepLeftReadInOrder(),
        step->getLeftKeys(),
        step->getRightKeys(),
        step->getKeyIdsNullSafe(),
        step->getFilter(),
        step->isHasUsing(),
        step->getRequireRightKeys(),
        ASOFJoinInequality::GreaterOrEquals,
        DistributionType::UNKNOWN,
        JoinAlgorithm::DEFAULT,
        step->isMagic(),
        step->isOrdered(),
        step->isSimpleReordered(),
        step->getRuntimeFilterBuilders());
    auto new_semi_node = std::make_shared<JoinStepExtNode>(
        rule_context.context->getOptimizerContext()->nextNodeId(),
        new_semi_step,
        PlanNodes{semi_join_node->getChildren()[0]->getChildren()[0], semi_join_node->getChildren()[1]});

    step = join_node->getStep();
    streams = {new_semi_step->getOutputStream(), join_node->getChildren()[1]->getCurrentDataStream()};
    auto output_step = std::make_shared<JoinStepExt>(
        streams,
        semi_join_node->getStep()->getOutputStream(),
        step->getKind(),
        step->getStrictness(),
        step->getMaxStreams(),
        step->getKeepLeftReadInOrder(),
        step->getLeftKeys(),
        step->getRightKeys(),
        step->getKeyIdsNullSafe(),
        step->getFilter(),
        step->isHasUsing(),
        step->getRequireRightKeys(),
        ASOFJoinInequality::GreaterOrEquals,
        DistributionType::UNKNOWN,
        JoinAlgorithm::DEFAULT,
        false,
        step->isOrdered(),
        step->isSimpleReordered(),
        step->getRuntimeFilterBuilders());
    return PlanNodeBase::createPlanNode(
        rule_context.context->getOptimizerContext()->nextNodeId(), output_step, PlanNodes{new_semi_node, join_node->getChildren()[1]});
}

ConstRefPatternPtr SemiJoinPushDownProjection::getPattern() const
{
    static auto pattern = Patterns::join()
        .matchingStep<JoinStepExt>([](const JoinStepExt & s) {
            return s.getKind() == JoinKind::Left
                && (s.getStrictness() == JoinStrictness::Semi || s.getStrictness() == JoinStrictness::Anti);
        })
        .with(Patterns::project().with(Patterns::any()), Patterns::any())
        .result();
    return pattern;
}

TransformResult SemiJoinPushDownProjection::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    const auto * join_step = dynamic_cast<const JoinStepExt *>(node->getStep().get());
    auto & projection = node->getChildren()[0];
    const auto * projection_step = dynamic_cast<const ProjectionStepExt *>(projection->getStep().get());

    auto identities = Utils::extractIdentities(*projection_step);
    for (const auto & left_key : join_step->getLeftKeys())
    {
        if (!identities.contains(left_key))
            return {};
    }
    for (const auto & symbol : SymbolsExtractor::extract(join_step->getFilter()))
    {
        // check symbol comes from left child and is simple identites
        if (projection_step->getInputStreams()[0].header.has(symbol) && !identities.contains(symbol))
            return {};
    }

    DataStream output_stream;
    for (const auto & col : node->getChildren()[0]->getChildren()[0]->getCurrentDataStream().header)
        output_stream.header.insert(col);
    for (const auto & col : node->getChildren()[1]->getCurrentDataStream().header)
    {
        // any inner join cotains symbols from right
        if (node->getCurrentDataStream().header.has(col.name))
            output_stream.header.insert(col);
    }

    auto mapper = SymbolMapper::simpleMapper(identities);

    auto context = rule_context.context;
    auto new_join_step = std::make_shared<JoinStepExt>(
        DataStreams{projection->getChildren()[0]->getStep()->getOutputStream(), node->getChildren()[1]->getStep()->getOutputStream()},
        output_stream,
        join_step->getKind(),
        join_step->getStrictness(),
        join_step->getMaxStreams(),
        join_step->getKeepLeftReadInOrder(),
        mapper.map(join_step->getLeftKeys()),
        join_step->getRightKeys(),
        join_step->getKeyIdsNullSafe(),
        mapper.map(join_step->getFilter()),
        join_step->isHasUsing(),
        join_step->getRequireRightKeys(),
        ASOFJoinInequality::GreaterOrEquals,
        DistributionType::UNKNOWN,
        JoinAlgorithm::DEFAULT,
        join_step->isMagic(),
        join_step->isOrdered(),
        join_step->isSimpleReordered(),
        join_step->getRuntimeFilterBuilders());


    // create new projection (add remaining symbols form right side if join is any join)
    Assignments assignments;
    NameToType name_to_type;
    for (const auto & item : projection_step->getAssignments())
        assignments.emplace(item.first, item.second);
    for (const auto & item : projection_step->getNameToType())
        name_to_type.emplace(item.first, item.second);
    // add remaining symbols
    for (const auto & output : ToNameToType(join_step->getOutputStream().header.getColumnsWithTypeAndName()))
    {
        if (!assignments.contains(output.first))
        {
            assignments.emplace(output.first, std::make_shared<ASTIdentifier>(output.first));
            name_to_type.emplace(output);
        }
    }

    auto new_projection_step = std::make_shared<ProjectionStepExt>(new_join_step->getOutputStream(), assignments, name_to_type);

    return TransformResult{
        PlanNodeBase::createPlanNode(
            context->getOptimizerContext()->nextNodeId(),
            new_projection_step,
            {PlanNodeBase::createPlanNode(context->getOptimizerContext()->nextNodeId(), new_join_step, {projection->getChildren()[0], node->getChildren()[1]})}),
        true};
}

ConstRefPatternPtr SemiJoinPushDownAggregate::getPattern() const
{
    static auto pattern = Patterns::join()
        .matchingStep<JoinStepExt>([](const JoinStepExt & s) {
            return s.getKind() == JoinKind::Left
                && (s.getStrictness() == JoinStrictness::Semi || s.getStrictness() == JoinStrictness::Anti);
        })
        .with(Patterns::aggregating().with(Patterns::any()), Patterns::any())
        .result();
    return pattern;
}

TransformResult SemiJoinPushDownAggregate::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    const auto * join_step = dynamic_cast<const JoinStepExt *>(node->getStep().get());

    for (const auto & col : node->getChildren()[1]->getCurrentDataStream().header)
    {
        // any inner join output contain symbols from right, so it cannot pushdown aggregate
        if (node->getCurrentDataStream().header.has(col.name))
            return {};
    }

    auto & aggregate = node->getChildren()[0];
    const auto * aggregating_step = dynamic_cast<const AggregatingStepExt *>(aggregate->getStep().get());

    auto identities = std::unordered_set<String>(aggregating_step->getKeys().begin(), aggregating_step->getKeys().end());
    for (const auto & left_key : join_step->getLeftKeys())
    {
        auto it = identities.find(left_key);
        if (it == identities.end())
            return {};
    }
    for (const auto & symbol : SymbolsExtractor::extract(join_step->getFilter()))
    {
        // check symbol comes from left child and is simple identites
        if (aggregating_step->getInputStreams()[0].header.has(symbol) && !identities.contains(symbol))
            return {};
    }

    auto context = rule_context.context;
    auto new_join_step = std::make_shared<JoinStepExt>(
        DataStreams{aggregate->getChildren()[0]->getStep()->getOutputStream(), node->getChildren()[1]->getStep()->getOutputStream()},
        aggregate->getChildren()[0]->getStep()->getOutputStream(),
        join_step->getKind(),
        join_step->getStrictness(),
        join_step->getMaxStreams(),
        join_step->getKeepLeftReadInOrder(),
        join_step->getLeftKeys(),
        join_step->getRightKeys(),
        join_step->getKeyIdsNullSafe(),
        join_step->getFilter(),
        join_step->isHasUsing(),
        join_step->getRequireRightKeys(),
        ASOFJoinInequality::GreaterOrEquals,
        DistributionType::UNKNOWN,
        JoinAlgorithm::DEFAULT,
        join_step->isMagic(),
        join_step->isOrdered(),
        join_step->isSimpleReordered(),
        join_step->getRuntimeFilterBuilders());

    return TransformResult{
        PlanNodeBase::createPlanNode(
            context->getOptimizerContext()->nextNodeId(),
            aggregate->getStep(),
            {PlanNodeBase::createPlanNode(context->getOptimizerContext()->nextNodeId(), new_join_step, {aggregate->getChildren()[0], node->getChildren()[1]})}),
        true};
}
}
