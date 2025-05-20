#include <Query/Optimizer/Rule/Rewrite/PullProjectionOnJoinThroughJoin.h>

#include <Query/Optimizer/Rule/Patterns.h>
#include <Query/Optimizer/Utils.h>
#include <Query/Processors/QueryPlan/JoinStepExt.h>
#include <Query/Processors/QueryPlan/ProjectionStepExt.h>
#include <Query/Planner/SymbolMapper.h>
#include <Query/Core/BlockHelper.h>

namespace DB
{

static std::optional<PlanNodePtr> tryPushJoinThroughLeftProjection(
    const PlanNodePtr & join,
    const PlanNodePtr & join_left_project,
    const PlanNodePtr & join_right_node,
    const PlanNodePtr & children,
    Context & context)
{
    const auto * join_step = dynamic_cast<const JoinStepExt *>(join->getStep().get());
    const auto * project_step = dynamic_cast<const ProjectionStepExt *>(join_left_project->getStep().get());

    NameSet left_names = BlockHelper::getNameSet(join_step->getInputStreams()[0].header);

    std::unordered_map<String, String> projection_mapping;
    for (const auto & assignment : project_step->getAssignments())
    {
        if (getAstType(assignment.second) == ASTType::ASTIdentifier)
        {
            const ASTIdentifier & identifier = assignment.second->as<ASTIdentifier &>();
            if (left_names.contains(assignment.first))
            {
                projection_mapping.emplace(assignment.first, identifier.name());
            }
        }
    }

    if (left_names.size() != projection_mapping.size())
    {
        return {};
    }

    // construct join
    SymbolMapper symbol_mapper = SymbolMapper::simpleMapper(projection_mapping);
    auto mapped_join = symbol_mapper.map(*join_step);

    // construct post projection(assignments including symbols which are not join outputs will be pruned)
    auto join_outputs = BlockHelper::getNameSet(mapped_join->getOutputStream().header);
    Assignments assignments;
    NameToType name_to_type;
    const auto & old_name_to_type = project_step->getNameToType();

    for (const auto & old_assignment: project_step->getAssignments())
    {
        if (join_outputs.count(old_assignment.second->as<ASTIdentifier &>().name()))
        {
            assignments.emplace_back(old_assignment);
            name_to_type[old_assignment.first] = old_name_to_type.at(old_assignment.first);
        }
    }

    for (const auto & name_and_type : join_right_node->getStep()->getOutputStream().header)
    {
        if (join_outputs.count(name_and_type.name))
        {
            assignments.emplace_back(name_and_type.name, std::make_shared<ASTIdentifier>(name_and_type.name));
            name_to_type.emplace(name_and_type.name, name_and_type.type);
        }
    }
    auto new_project_step
        = std::make_shared<ProjectionStepExt>(mapped_join->getOutputStream(), assignments, name_to_type, project_step->isFinalProject(), project_step->isIndexProject());

    return PlanNodeBase::createPlanNode(
        context.getOptimizerContext()->nextNodeId(), new_project_step, {PlanNodeBase::createPlanNode(context.getOptimizerContext()->nextNodeId(), mapped_join, {children, join_right_node})});
}

static std::optional<PlanNodePtr> tryPushJoinThroughRightProjection(
    const PlanNodePtr & join,
    const PlanNodePtr & join_right_project,
    const PlanNodePtr & join_left_node,
    const PlanNodePtr & children,
    Context & context)
{
    const auto * join_step = dynamic_cast<const JoinStepExt *>(join->getStep().get());
    const auto * project_step = dynamic_cast<const ProjectionStepExt *>(join_right_project->getStep().get());

    NameSet right_names = BlockHelper::getNameSet(join_step->getInputStreams()[1].header);

    std::unordered_map<String, String> projection_mapping;
    for (const auto & assignment : project_step->getAssignments())
    {
        if (getAstType(assignment.second) == ASTType::ASTIdentifier)
        {
            const ASTIdentifier & identifier = assignment.second->as<ASTIdentifier &>();
            if (right_names.contains(assignment.first))
            {
                projection_mapping.emplace(assignment.first, identifier.name());
            }
        }
    }

    if (right_names.size() != projection_mapping.size())
    {
        return {};
    }

    // construct join
    SymbolMapper symbol_mapper = SymbolMapper::simpleMapper(projection_mapping);
    auto mapped_join = symbol_mapper.map(*join_step);

    // construct post projection(assignments including symbols which are not join outputs will be pruned)
    auto join_outputs = BlockHelper::getNameSet(mapped_join->getOutputStream().header);
    Assignments assignments;
    NameToType name_to_type;
    const auto & old_name_to_type = project_step->getNameToType();

    for (const auto & old_assignment: project_step->getAssignments())
    {
        if (join_outputs.count(old_assignment.second->as<ASTIdentifier &>().name()))
        {
            assignments.emplace_back(old_assignment);
            name_to_type[old_assignment.first] = old_name_to_type.at(old_assignment.first);
        }
    }

    for (const auto & name_and_type : join_left_node->getStep()->getOutputStream().header)
    {
        if (join_outputs.count(name_and_type.name))
        {
            assignments.emplace_back(name_and_type.name, std::make_shared<ASTIdentifier>(name_and_type.name));
            name_to_type.emplace(name_and_type.name, name_and_type.type);
        }
    }
    auto new_project_step
        = std::make_shared<ProjectionStepExt>(join_step->getOutputStream(), assignments, name_to_type, project_step->isFinalProject(), project_step->isIndexProject());

    return PlanNodeBase::createPlanNode(
        context.getOptimizerContext()->nextNodeId(), new_project_step, {PlanNodeBase::createPlanNode(context.getOptimizerContext()->nextNodeId(), mapped_join, {join_left_node, children})});
}

static bool isProjectionWithJoin(const PlanNodePtr & node)
{
    return getQueryPlanStepType(node->getStep()) == QueryPlanStepType::ProjectionStepExt
        && getQueryPlanStepType(node->getChildren()[0]->getStep()) == QueryPlanStepType::JoinStepExt;
}

ConstRefPatternPtr PullProjectionOnJoinThroughJoin::getPattern() const
{
    static auto pattern = Patterns::join().withAny(
        Patterns::project()
            // identity projection will be inlined into join
            .matchingStep<ProjectionStepExt>([](const auto & step) { return !Utils::isIdentity(step.getAssignments()); })
            .withSingle(Patterns::join())).result();
    return pattern;
}

TransformResult PullProjectionOnJoinThroughJoin::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto & join_step = dynamic_cast<const JoinStepExt &>(*node->getStep());

    if (isProjectionWithJoin(node->getChildren()[0])
        && (isInner(join_step.getKind()) || isLeft(join_step.getKind())))
    {
        auto ret = tryPushJoinThroughLeftProjection(
            node, node->getChildren()[0], node->getChildren()[1], node->getChildren()[0]->getChildren()[0], *context.context);
        return TransformResult::of(ret);
    }

    if (isProjectionWithJoin(node->getChildren()[1])
        && (isInner(join_step.getKind()) || isRight(join_step.getKind())))
    {
        auto ret = tryPushJoinThroughRightProjection(
            node, node->getChildren()[1], node->getChildren()[0], node->getChildren()[1]->getChildren()[0], *context.context);
        return TransformResult::of(ret);
    }

    return {};
}

}
