#include <Query/Optimizer/Rule/Rewrite/RemoveRedundantRules.h>

#include <DataTypes/DataTypeNullable.h>
#include <Query/Optimizer/ExpressionInterpreter.h>
#include <Query/Optimizer/PlanNodeCardinality.h>
#include <Query/Optimizer/PredicateUtils.h>
#include <Query/Optimizer/Utils.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/IAST_fwd.h>
#include <Query/Processors/QueryPlan/ApplyStepExt.h>
#include <Query/Processors/QueryPlan/CTERefStepExt.h>
#include <Query/Processors/QueryPlan/FilterStepExt.h>
#include <Query/Processors/QueryPlan/ProjectionStepExt.h>
#include <Query/Processors/QueryPlan/UnionStepExt.h>
#include <Query/Processors/QueryPlan/CTERefStepExt.h>

namespace DB
{
TransformResult RemoveRedundantFilter::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto & context = rule_context.context;
    const auto * step = dynamic_cast<const FilterStepExt *>(node->getStep().get());
    auto expr = step->getFilter();

    if (const auto * literal = expr->as<ASTLiteral>())
    {
        const auto cols = step->getInputStreams()[0].header.getColumnsWithTypeAndName();
        NameToType name_to_type;
        for (const auto & col: cols)
        {
            name_to_type.emplace(col.name, col.type);
        }
        auto result = ExpressionInterpreter::evaluateConstantExpression(expr, name_to_type, context);
        if (result.has_value() && result->second.isNull())
        {
            auto null_step = std::make_unique<ReadNothingStep>(step->getOutputStream().header);
            auto null_node = PlanNodeBase::createPlanNode(context->getOptimizerContext()->nextNodeId(), std::move(null_step));
            return {null_node};
        }

        UInt64 value;
        if (literal->value.tryGet(value) && value == 0)
        {
            auto null_step = std::make_unique<ReadNothingStep>(step->getOutputStream().header);
            auto null_node = PlanNodeBase::createPlanNode(context->getOptimizerContext()->nextNodeId(), std::move(null_step));
            return {null_node};
        }
        if (literal->value.tryGet(value) && value == 1)
        {
            return node->getChildren()[0];
        }
    }

    return {};
}

TransformResult RemoveRedundantUnion::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto & context = rule_context.context;
    const auto * step = dynamic_cast<const UnionStepExt *>(node->getStep().get());

    DataStreams inputs;
    PlanNodes children;
    OutputToInputs output_to_inputs;
    int index = 0;
    for (auto & child : node->getChildren())
    {
        if (!dynamic_cast<const ReadNothingStep *>(child->getStep().get()))
        {
            inputs.emplace_back(child->getStep()->getOutputStream());
            children.emplace_back(child);
            for (const auto & output_to_input : step->getOutToInputs())
                output_to_inputs[output_to_input.first].push_back(output_to_input.second[index]);
        }
        ++index;
    }

    if (children.empty())
    {
        auto null_step = std::make_unique<ReadNothingStep>(step->getOutputStream().header);
        return PlanNodeBase::createPlanNode(context->getOptimizerContext()->nextNodeId(), std::move(null_step));
    }

    // local union equals to local gather (make thread single), can't rewrite to projection.
    if (children.size() == 1 && !step->isLocal())
    {
        auto input_columns = children[0]->getStep()->getOutputStream().header;
        Assignments assignments;
        NameToType name_to_type;
        for (const auto & output_to_input : step->getOutToInputs())
        {
            String output = output_to_input.first;
            for (const auto & input : output_to_input.second)
            {
                for (auto & input_column : input_columns)
                {
                    if (input == input_column.name)
                    {
                        Assignment column{output, std::make_shared<ASTIdentifier>(input_column.name)};
                        assignments.emplace_back(column);
                        name_to_type[output] = input_column.type;
                    }
                }
            }
        }
        auto project_step = std::make_shared<ProjectionStepExt>(children[0]->getStep()->getOutputStream(), assignments, name_to_type);
        return PlanNodeBase::createPlanNode(context->getOptimizerContext()->nextNodeId(), std::move(project_step), children, node->getStatistics());
    }

    if (children.size() != node->getChildren().size())
    {
        auto union_step
            = std::make_unique<UnionStepExt>(inputs, step->getOutputStream(), output_to_inputs, step->getMaxThreads(), step->isLocal());
        return PlanNodeBase::createPlanNode(context->getOptimizerContext()->nextNodeId(), std::move(union_step), children, node->getStatistics());
    }

    return {};
}

TransformResult RemoveRedundantProjection::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    auto * projection_node = dynamic_cast<ProjectionStepExtNode *>(node.get());
    if (!projection_node)
        return {};
    const auto & step = *projection_node->getStep();

    if (Utils::isIdentity(step))
    {
        const DataStream & output_stream = step.getOutputStream();
        const DataStream & source_output_stream = node->getChildren()[0]->getStep()->getOutputStream();

        // remove duplicated columns
        std::unordered_set<std::string> output_symbols;
        for (const auto & column : output_stream.header)
        {
            output_symbols.emplace(column.name);
        }
        std::unordered_set<std::string> source_output_symbols;
        for (const auto & column : source_output_stream.header)
        {
            source_output_symbols.emplace(column.name);
        }
        if (output_symbols == source_output_symbols)
        {
            return node->getChildren()[0];
        }
    }

    if (getQueryPlanStepType(node->getChildren()[0]->getStep()) == QueryPlanStepType::ReadNothingStep)
    {
        return PlanNodeBase::createPlanNode(
            context.context->getOptimizerContext()->nextNodeId(), std::make_shared<ReadNothingStep>(node->getStep()->getOutputStream().header));
    }
    return {};
}

TransformResult RemoveRedundantEnforceSingleRow::transformImpl(PlanNodePtr node, const Captures &, RuleContext &)
{
    if (PlanNodeCardinality::isScalar(*node->getChildren()[0]))
    {
        return node->getChildren()[0];
    }
    return {};
}

ConstRefPatternPtr RemoveRedundantCrossJoin::getPattern() const
{
    static auto pattern = Patterns::join()
        .matchingStep<JoinStepExt>([](const JoinStepExt & s) { return s.getKind() == JoinKind::Cross; })
        .with(Patterns::any(), Patterns::any())
        .result();
    return pattern;
}

TransformResult RemoveRedundantCrossJoin::transformImpl(PlanNodePtr node, const Captures &, RuleContext &)
{
    // normal case
    if (node->getChildren()[0]->getStep()->getOutputStream().header.columns() == 0
        && PlanNodeCardinality::isScalar(*node->getChildren()[0]))
    {
        return node->getChildren()[1];
    }
    if (node->getChildren()[1]->getStep()->getOutputStream().header.columns() == 0
        && PlanNodeCardinality::isScalar(*node->getChildren()[1]))
    {
        return node->getChildren()[0];
    }

    return {};
}

ConstRefPatternPtr RemoveReadNothing::getPattern() const
{
    static auto pattern = Patterns::any().withSingle(Patterns::readNothing()).result();
    return pattern;
}

TransformResult RemoveReadNothing::transformImpl(PlanNodePtr, const Captures &, RuleContext &)
{
    return {};
}

ConstRefPatternPtr RemoveRedundantJoin::getPattern() const
{
    static auto pattern = Patterns::join()
        .matchingStep<JoinStepExt>(
            [](const JoinStepExt & s) { return s.getKind() == JoinKind::Inner || s.getKind() == JoinKind::Cross; })
        .withAny(Patterns::readNothing())
        .result();
    return pattern;
}

TransformResult RemoveRedundantJoin::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    if (getQueryPlanStepType(node->getChildren()[0]->getStep()) == QueryPlanStepType::ReadNothingStep
        || getQueryPlanStepType(node->getChildren()[1]->getStep()) == QueryPlanStepType::ReadNothingStep)
    {
        return PlanNodeBase::createPlanNode(
            context.context->getOptimizerContext()->nextNodeId(), std::make_shared<ReadNothingStep>(node->getStep()->getOutputStream().header));
    }
    return {};
}

ConstRefPatternPtr RemoveRedundantOuterJoin::getPattern() const
{
    static auto pattern = Patterns::join()
        .matchingStep<JoinStepExt>(
            [](const JoinStepExt & s) { return isLeft(s.getKind()) || isRight(s.getKind()); })
        .result();
    return pattern;
}

TransformResult RemoveRedundantOuterJoin::transformImpl(PlanNodePtr node, const Captures &, RuleContext &)
{
    auto * join_node = dynamic_cast<JoinStepExtNode *>(node.get());

    auto all_type_nothing = [](Block block, Names keys) {
        for (const auto & key : keys)
        {
            auto type = block.getByName(key).type;
            if (removeNullable(recursiveRemoveLowCardinality(type))->getTypeId() != TypeIndex::Nothing)
            {
                return false;
            }
        }
        return true;
    };

    if (join_node)
    {
        auto step = join_node->getStep();
        if (isLeft(step->getKind()))
        {
            if (getQueryPlanStepType(node->getChildren()[1]->getStep()) == QueryPlanStepType::ReadNothingStep
                || all_type_nothing(node->getChildren()[1]->getStep()->getOutputStream().header, step->getRightKeys()))
            {
                // todo: bc, add project to add joined columns
                return node->getChildren()[0];
            }
        }
        if (isRight(step->getKind()))
        {
            if (getQueryPlanStepType(node->getChildren()[0]->getStep()) == QueryPlanStepType::ReadNothingStep
                || all_type_nothing(node->getChildren()[0]->getStep()->getOutputStream().header, step->getLeftKeys()))
            {
                // todo: bc, add project to add joined columns
                return node->getChildren()[1];
            }
        }
    }
    return {};
}

ConstRefPatternPtr RemoveRedundantLimit::getPattern() const
{
    static auto pattern = Patterns::limit().result();
    return pattern;
}

TransformResult RemoveRedundantLimit::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    auto * limit_node = dynamic_cast<LimitStepExtNode *>(node.get());
    if (!limit_node->getStep()->hasPreparedParam() && limit_node->getStep()->getLimit() == 0)
    {
        auto null_step = std::make_unique<ReadNothingStep>(limit_node->getStep()->getOutputStream().header);
        auto null_node = PlanNodeBase::createPlanNode(context.context->getOptimizerContext()->nextNodeId(), std::move(null_step));
        return {null_node};
    }

    return {};
}

ConstRefPatternPtr RemoveRedundantAggregate::getPattern() const
{
    static auto pattern = Patterns::aggregating().result();
    return pattern;
}

TransformResult RemoveRedundantAggregate::transformImpl(PlanNodePtr, const Captures &, RuleContext &)
{
    return {};
}

ConstRefPatternPtr RemoveRedundantAggregateWithReadNothing::getPattern() const
{
    static auto pattern = Patterns::aggregating().matchingStep<AggregatingStepExt>([](const AggregatingStepExt & s) { return !s.getKeys().empty(); }).withSingle(Patterns::readNothing()).result();
    return pattern;
}

TransformResult RemoveRedundantAggregateWithReadNothing::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    auto * step = dynamic_cast<AggregatingStepExt *>(node->getStep().get());
    auto read_nothing_step = std::make_shared<ReadNothingStep>(step->getOutputStream().header);
    auto read_nothing_node = PlanNodeBase::createPlanNode(context.context->getOptimizerContext()->nextNodeId(), std::move(read_nothing_step), {});
    return {read_nothing_node};
}

ConstRefPatternPtr RemoveRedundantTwoApply::getPattern() const
{
    static auto pattern = Patterns::filter()
        .withSingle(
            Patterns::apply()
                .matchingStep<ApplyStepExt>([](const ApplyStepExt & apply) { return apply.getSubqueryType() == ApplyStepExt::SubqueryType::IN; })
                .with(
                    Patterns::apply()
                        .matchingStep<ApplyStepExt>(
                            [](const ApplyStepExt & apply) { return apply.getSubqueryType() == ApplyStepExt::SubqueryType::IN; })
                        .with(Patterns::any(), Patterns::cte()),
                    Patterns::project().withSingle(Patterns::filter().withSingle(Patterns::join().with(Patterns::any(), Patterns::cte())))))
        .result();
    return pattern;
}

TransformResult RemoveRedundantTwoApply::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    auto * filter = dynamic_cast<FilterStepExt *>(node->getStep().get());

    auto * first_apply = dynamic_cast<ApplyStepExt *>(node->getChildren()[0]->getStep().get());
    auto * first_cte = dynamic_cast<CTERefStepExt *>(
        node->getChildren()[0]->getChildren()[1]->getChildren()[0]->getChildren()[0]->getChildren()[1]->getStep().get());
    auto * second_apply = dynamic_cast<ApplyStepExt *>(node->getChildren()[0]->getChildren()[0]->getStep().get());
    auto * second_cte = dynamic_cast<CTERefStepExt *>(node->getChildren()[0]->getChildren()[0]->getChildren()[1]->getStep().get());

    if (first_cte->getId() != second_cte->getId())
        return {};


    auto conjuncts = PredicateUtils::extractConjuncts(filter->getFilter());
    bool match_first = false;
    bool match_second = false;
    for (const auto & conjunct : conjuncts)
    {
        if (const auto * id = conjunct->as<ASTIdentifier>())
        {
            if (id->getColumnName() == first_apply->getAssignment().first)
            {
                match_first = true;
            }
            if (id->getColumnName() == second_apply->getAssignment().first)
            {
                match_second = true;
            }
        }
    }

    if (match_first && match_second)
    {
        auto second_apply_left = node->getChildren()[0]->getChildren()[0]->getChildren()[0];
        auto new_apply = std::make_shared<ApplyStepExt>(
            DataStreams{second_apply_left->getCurrentDataStream(), first_apply->getInputStreams()[1]},
            first_apply->getCorrelation(),
            first_apply->getApplyType(),
            first_apply->getSubqueryType(),
            first_apply->getAssignment(),
            first_apply->getOuterColumns(),
            first_apply->supportSemiAnti());
        auto new_apply_node = PlanNodeBase::createPlanNode(
            context.context->getOptimizerContext()->nextNodeId(), new_apply, {second_apply_left, node->getChildren()[0]->getChildren()[1]});


        std::vector<ConstASTPtr> new_filter;
        for (const auto & conjunct : conjuncts)
        {
            if (const auto * id = conjunct->as<ASTIdentifier>())
            {
                if (id->getColumnName() == second_apply->getAssignment().first)
                {
                    continue;
                }
            }
            new_filter.emplace_back(conjunct);
        }

        ConstASTPtr filter_ast = PredicateUtils::combineConjuncts(new_filter);
        auto new_filter_step = std::make_shared<FilterStepExt>(new_apply_node->getCurrentDataStream(), filter_ast);
        return PlanNodeBase::createPlanNode(context.context->getOptimizerContext()->nextNodeId(), new_filter_step, {new_apply_node});
    }


    return {};
}

}
