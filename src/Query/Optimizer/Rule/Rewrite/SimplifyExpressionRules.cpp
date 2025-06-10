#include <Query/Optimizer/Rule/Rewrite/SimplifyExpressionRules.h>

#include <Core/Joins.h>
#include <Query/Interpreters/JoinUtilsExt.h>
#include <Query/Optimizer/ExpressionInterpreter.h>
#include <Query/Optimizer/Property/ConstantsDeriver.h>
#include <Query/Optimizer/Rule/Patterns.h>
#include <Query/Optimizer/SimplifyExpressions.h>
#include <Query/Optimizer/UnwrapCastInComparison.h>
#include <Query/Optimizer/Utils.h>
#include <Parsers/formatAST.h>

#include <Query/Processors/QueryPlan/FilterStepExt.h>
#include <Query/Processors/QueryPlan/JoinStepExt.h>

namespace DB
{
ConstRefPatternPtr CommonPredicateRewriteRule::getPattern() const
{
    static auto pattern = Patterns::filter().result();
    return pattern;
}

TransformResult CommonPredicateRewriteRule::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto & context = rule_context.context;
    auto * old_filter_node = dynamic_cast<FilterStepExtNode *>(node.get());
    if (!old_filter_node)
        return {};

    const auto & step = *old_filter_node->getStep();
    auto predicate = step.getFilter();

    ConstASTPtr rewritten = CommonPredicatesRewriter::rewrite(predicate, context);
    if (rewritten->getColumnName() == predicate->getColumnName())
    {
        return {};
    }

    auto filter_step
        = std::make_shared<FilterStepExt>(node->getChildren()[0]->getStep()->getOutputStream(), rewritten, step.removesFilterColumn());
    auto filter_node = FilterStepExtNode::createPlanNode(context->getOptimizerContext()->nextNodeId(), std::move(filter_step), PlanNodes{node->getChildren()[0]});

    return filter_node;
}

ConstRefPatternPtr CommonJoinFilterRewriteRule::getPattern() const
{
    static auto pattern = Patterns::join().matchingStep<JoinStepExt>([&](const JoinStepExt & s) { return !PredicateUtils::isTruePredicate(s.getFilter()); }).result();
    return pattern;
}

TransformResult CommonJoinFilterRewriteRule::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto & context = rule_context.context;
    const auto & step_ptr = node->getStep();
    const auto & step = dynamic_cast<const JoinStepExt &>(*step_ptr);

    auto filter = step.getFilter();
    ConstASTPtr rewritten = CommonPredicatesRewriter::rewrite(filter, context);

    if (rewritten->getColumnName() == filter->getColumnName())
    {
        return {};
    }

    QueryPlanStepPtr join_step = std::make_shared<JoinStepExt>(
        step.getInputStreams(),
        step.getOutputStream(),
        step.getKind(),
        step.getStrictness(),
        step.getMaxStreams(),
        step.getKeepLeftReadInOrder(),
        step.getLeftKeys(),
        step.getRightKeys(),
        step.getKeyIdsNullSafe(),
        rewritten,
        step.isHasUsing(),
        step.getRequireRightKeys(),
        step.getAsofInequality(),
        step.getDistributionType(),
        JoinAlgorithm::DEFAULT,
        false,
        step.isOrdered(),
        step.isSimpleReordered(),
        step.getRuntimeFilterBuilders());

    PlanNodePtr join_node = PlanNodeBase::createPlanNode(context->getOptimizerContext()->nextNodeId(), std::move(join_step), node->getChildren());
    return join_node;
}

ConstRefPatternPtr SwapPredicateRewriteRule::getPattern() const
{
    static auto pattern = Patterns::filter().result();
    return pattern;
}

TransformResult SwapPredicateRewriteRule::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto & context = rule_context.context;
    if (!context->getOptimizerContext()->getSettingsRef().enable_swap_predicate_rewrite)
    {
        return {};
    }
    auto * old_filter_node = dynamic_cast<FilterStepExtNode *>(node.get());
    if (!old_filter_node)
        return {};

    const auto & step = *old_filter_node->getStep();
    const auto & predicate = step.getFilter();

    ConstASTPtr rewritten = SwapPredicateRewriter::rewrite(predicate, context);
    if (rewritten->getColumnName() == predicate->getColumnName())
    {
        return {};
    }

    auto filter_step
        = std::make_shared<FilterStepExt>(node->getChildren()[0]->getStep()->getOutputStream(), rewritten, step.removesFilterColumn());
    auto filter_node = PlanNodeBase::createPlanNode(node->getId(), std::move(filter_step), PlanNodes{node->getChildren()[0]});

    return filter_node;
}

ConstRefPatternPtr SimplifyPredicateRewriteRule::getPattern() const
{
    static auto pattern = Patterns::filter().result();
    return pattern;
}

TransformResult SimplifyPredicateRewriteRule::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto & context = rule_context.context;
    auto * old_filter_node = dynamic_cast<FilterStepExtNode *>(node.get());
    if (!old_filter_node)
        return {};

    const auto & step = *old_filter_node->getStep();
    auto predicate = step.getFilter();

    ExpressionInterpreter::IdentifierValues constants;
    if (context->getOptimizerContext()->getSettingsRef().enable_simplify_expression_by_derived_constant)
    {
        auto derived_constants = ConstantsDeriver::deriveConstantsFromTree(node->getChildren().at(0), rule_context.cte_info, context);
        for (const auto & [name, field_with_type] : derived_constants.getValues())
            constants.emplace(name, field_with_type.value);
    }
    auto cols_with_type_and_name = step.getOutputStream().header.getColumnsWithTypeAndName();
    NameToType name_and_types;
    for (const auto & type_and_name : cols_with_type_and_name)
    {
        name_and_types.emplace(type_and_name.name, type_and_name.type);
    }
    ConstASTPtr rewritten
        = ExpressionInterpreter::optimizePredicate(predicate, name_and_types, context, constants);

    if (PredicateUtils::isTruePredicate(rewritten))
        return node->getChildren()[0];

    if (rewritten->getColumnName() == predicate->getColumnName())
        return {};

    if (const auto * literal = rewritten->as<ASTLiteral>())
    {
        const auto cols =  step.getInputStreams()[0].header.getColumnsWithTypeAndName();
        NameToType name_to_type;
        for (const auto & col: cols)
        {
            name_to_type.emplace(col.name, col.type);
        }
        auto result = ExpressionInterpreter::evaluateConstantExpression(rewritten, name_to_type, context);
        if (result.has_value() && result->second.isNull())
        {
            auto null_step = std::make_unique<ReadNothingStep>(step.getOutputStream().header);
            auto null_node = PlanNodeBase::createPlanNode(context->getOptimizerContext()->nextNodeId(), std::move(null_step));
            return {null_node};
        }

        UInt64 value;
        if (literal->value.tryGet(value) && value == 0)
        {
            auto null_step = std::make_unique<ReadNothingStep>(step.getOutputStream().header);
            auto null_node = PlanNodeBase::createPlanNode(context->getOptimizerContext()->nextNodeId(), std::move(null_step));
            return {null_node};
        }
        if (literal->value.tryGet(value) && value == 1)
        {
            return node->getChildren()[0];
        }
    }

    auto filter_step
        = std::make_shared<FilterStepExt>(node->getChildren()[0]->getStep()->getOutputStream(), rewritten, step.removesFilterColumn());
    auto filter_node = PlanNodeBase::createPlanNode(node->getId(), std::move(filter_step), PlanNodes{node->getChildren()[0]});

    return filter_node;
}

ConstRefPatternPtr UnWarpCastInPredicateRewriteRule::getPattern() const
{
    static auto pattern = Patterns::filter().result();
    return pattern;
}

TransformResult UnWarpCastInPredicateRewriteRule::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto & context = rule_context.context;
    auto * old_filter_node = dynamic_cast<FilterStepExtNode *>(node.get());
    if (!old_filter_node)
        return {};

    const auto & step = *old_filter_node->getStep();
    const auto & predicate = step.getFilter();
    const auto cols =  step.getOutputStream().header.getColumnsWithTypeAndName();
    NameToType column_types;
    for (const auto & col: cols)
    {
        column_types.emplace(col.name, col.type);
    }
    ASTPtr rewritten = unwrapCastInComparison(predicate, context, column_types);
    if (!rewritten)
    {
        rewritten = predicate->clone();
    }

    if (rewritten->getColumnName() == predicate->getColumnName())
    {
        return {};
    }

    auto filter_step
        = std::make_shared<FilterStepExt>(node->getChildren()[0]->getStep()->getOutputStream(), rewritten, step.removesFilterColumn());
    auto filter_node = PlanNodeBase::createPlanNode(node->getId(), std::move(filter_step), PlanNodes{node->getChildren()[0]});

    return filter_node;
}

ConstRefPatternPtr SimplifyJoinFilterRewriteRule::getPattern() const
{
    static auto pattern = Patterns::join()
                              .matchingStep<JoinStepExt>([](const JoinStepExt & s) { return !PredicateUtils::isTruePredicate(s.getFilter()); })
                              .result();
    return pattern;
}

TransformResult SimplifyJoinFilterRewriteRule::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto & context = rule_context.context;

    auto * old_join_node = dynamic_cast<JoinStepExtNode *>(node.get());
    if (!old_join_node)
        return {};

    const auto & step = *old_join_node->getStep();
    auto filter = step.getFilter();

    NamesAndTypes column_types;

    bool has_outer_join_semantic = (step.getStrictness() == JoinStrictness::Any) || (step.getStrictness() == JoinStrictness::All) || (step.getStrictness() == JoinStrictness::Asof);
    bool make_nullable_for_left = has_outer_join_semantic && isRightOrFull(step.getKind());
    bool make_nullable_for_right = has_outer_join_semantic && isLeftOrFull(step.getKind());

    auto type_with_nullable = [&](bool make_nullable, const NamesAndTypes & header) {
        if (make_nullable)
        {
            for (const auto & column : header)
            {
                if (JoinCommon::canBecomeNullable(column.type))
                {
                    NameAndTypePair name_and_type{column.name, JoinCommon::tryConvertTypeToNullable(column.type)};
                    column_types.emplace_back(name_and_type);
                }
                else
                {
                    column_types.emplace_back(column);
                }
            }
        }
        else
        {
            column_types.insert(column_types.end(), header.begin(), header.end());
        }
    };

    type_with_nullable(make_nullable_for_left, step.getInputStreams()[0].header.getNamesAndTypes());
    type_with_nullable(make_nullable_for_right, step.getInputStreams()[1].header.getNamesAndTypes());

    NameToType name_to_type;
    for (const auto & item : column_types)
        name_to_type.emplace(item.name, item.type);

    ASTPtr rewritten = ExpressionInterpreter::optimizePredicate(filter, name_to_type, context);

    if (rewritten->getColumnName() == filter->getColumnName())
    {
        return {};
    }

    auto join_step = std::make_shared<JoinStepExt>(
        step.getInputStreams(),
        step.getOutputStream(),
        step.getKind(),
        step.getStrictness(),
        step.getMaxStreams(),
        step.getKeepLeftReadInOrder(),
        step.getLeftKeys(),
        step.getRightKeys(),
        step.getKeyIdsNullSafe(),
        rewritten,
        step.isHasUsing(),
        step.getRequireRightKeys(),
        step.getAsofInequality(),
        step.getDistributionType(),
        JoinAlgorithm::DEFAULT,
        false,
        step.isOrdered(),
        step.isSimpleReordered(),
        step.getRuntimeFilterBuilders());
    PlanNodePtr join_node = PlanNodeBase::createPlanNode(node->getId(), std::move(join_step), node->getChildren());
    return join_node;
}

ConstRefPatternPtr SimplifyExpressionRewriteRule::getPattern() const
{
    static auto pattern = Patterns::project().result();
    return pattern;
}

TransformResult SimplifyExpressionRewriteRule::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto & context = rule_context.context;

    const auto * project = dynamic_cast<const ProjectionStepExt *>(node->getStep().get());
    if (!project)
        return {};

    Assignments assignments;
    NameToType name_to_type;

    const auto cols =  node->getChildren()[0]->getCurrentDataStream().header.getColumnsWithTypeAndName();
    NameToType column_types;
    for (const auto & col: cols)
    {
        column_types.emplace(col.name, col.type);
    }
    auto interpreter = rule_context.context->getOptimizerContext()->getSettingsRef().enable_simplify_predicate_in_projection
        ? ExpressionInterpreter::optimizedInterpreter(std::move(column_types), {}, context)
        : ExpressionInterpreter::basicInterpreter(std::move(column_types), context);
    bool rewrite = false;
    for (const auto & assignment : project->getAssignments())
    {
        auto res = interpreter.optimizeExpression(assignment.second);
        assignments.emplace_back(assignment.first, res.second);
        name_to_type.emplace(assignment.first, res.first);
        // auto output_types = project->getOutputStream().header.getNamesToTypes();
        // assert(res.first->equals(*output_types.at(assignment.first)));
        if (!ASTEquality::compareTree(assignments.back().second, assignment.second))
            rewrite = true;
    }
    if (!rewrite)
        return {};

    return PlanNodeBase::createPlanNode(
        node->getId(),
        std::make_shared<ProjectionStepExt>(
            node->getChildren()[0]->getStep()->getOutputStream(),
            assignments,
            name_to_type,
            project->isFinalProject(),
            project->isIndexProject()),
        PlanNodes{node->getChildren()[0]});
}

ConstRefPatternPtr MergePredicatesUsingDomainTranslator::getPattern() const
{
    static auto pattern = Patterns::filter().result();
    return pattern;
}

TransformResult MergePredicatesUsingDomainTranslator::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto & context = rule_context.context;
    const auto & settings = context->getOptimizerContext()->getSettingsRef();
    if (!settings.rewrite_predicate_by_domain)
        return {};

    auto * old_filter_node = dynamic_cast<FilterStepExtNode *>(node.get());
    const auto & step = *old_filter_node->getStep();
    auto predicate = step.getFilter()->clone();

    using ExtractionReuslt = DB::Predicate::ExtractionResult<ASTPtr>;
    using DomainTranslator = DB::Predicate::DomainTranslator<ASTPtr>;

    DomainTranslator domain_translator{context};
    ExtractionReuslt rewritten = domain_translator.getExtractionResult(predicate, step.getOutputStream().header.getNamesAndTypes());

    if (domain_translator.isIgnored() && !context->getOptimizerContext()->getSettingsRef().rewrite_complex_predicate_by_domain)
        return {};

    ASTPtr combine_extraction_result
        = PredicateUtils::combineConjuncts(ASTs{domain_translator.toPredicate(rewritten.tuple_domain), rewritten.remaining_expression});

    if (combine_extraction_result->getColumnName() == predicate->getColumnName())
        return {};

    auto filter_step = std::make_shared<FilterStepExt>(
        node->getChildren()[0]->getStep()->getOutputStream(), combine_extraction_result, step.removesFilterColumn());
    auto filter_node = PlanNodeBase::createPlanNode(node->getId(), std::move(filter_step), PlanNodes{node->getChildren()[0]});

    return filter_node;
}

}
