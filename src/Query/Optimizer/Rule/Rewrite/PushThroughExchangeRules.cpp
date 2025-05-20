#include <Query/Optimizer/Rule/Rewrite/PushThroughExchangeRules.h>

namespace DB
{
//ConstRefPatternPtr PushRuntimeFilterBuilderThroughExchange::getPattern() const
//{
//    static auto pattern = Patterns::project()
//        ->matchingStep<ProjectionStep>([](const auto & project) { return !project.getRuntimeFilters().empty(); })
//        ->withSingle(Patterns::exchange());
//}
//
//TransformResult PushRuntimeFilterBuilderThroughExchange::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
//{
//    const auto * project_step = dynamic_cast<const ProjectionStep *>(node->getStep().get());
//    const auto & exchange = node->getChildren()[0];
//
//    LinkedHashMap<String, RuntimeFilterBuildInfos> pushdown_runtime_filter;
//    LinkedHashMap<String, RuntimeFilterBuildInfos> remaining_runtime_filter;
//    for (const auto & runtime_filter : project_step->getRuntimeFilters())
//        if (Utils::isIdentity(runtime_filter.first, project_step->getAssignments().at(runtime_filter.first)))
//            pushdown_runtime_filter.emplace_back(runtime_filter);
//        else
//            remaining_runtime_filter.emplace_back(runtime_filter);
//
//    Assignments assignments;
//    NameToType name_to_type;
//    for (const auto & name_and_type : project_step->getInputStreams()[0].header)
//    {
//        assignments.emplace_back(name_and_type.name, std::make_shared<ASTIdentifier>(name_and_type.name));
//        name_to_type.emplace(name_and_type.name, name_and_type.type);
//    }
//
//    auto plan = PlanNodeBase::createPlanNode(
//        exchange->getId(),
//        exchange->getStep(),
//        {PlanNodeBase::createPlanNode(
//            context.context->nextNodeId(),
//            std::make_shared<ProjectionStep>(
//                project_step->getInputStreams()[0], assignments, name_to_type, false, pushdown_runtime_filter),
//            exchange->getChildren())});
//
//    if (project_step->isFinalProject() || !Utils::isIdentity(project_step->getAssignments())) {
//        return PlanNodeBase::createPlanNode(
//            node->getId(),
//            std::make_shared<ProjectionStep>(
//                project_step->getInputStreams()[0],
//                project_step->getAssignments(),
//                project_step->getNameToType(),
//                project_step->isFinalProject(),
//                remaining_runtime_filter),
//            {plan});
//    }
//    return plan;
//}
}
