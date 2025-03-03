#include <IO/Operators.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTExpressionList.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Query/Processors/QueryPlan/ProjectionStepExt.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

ProjectionStepExt::ProjectionStepExt(
    const DataStream & input_stream_, Assignments assignments_, NameToType name_to_type_, bool final_project_, bool index_project_)
    : ITransformingStep(input_stream_, {}, {}, true)
    , assignments(std::move(assignments_))
    , name_to_type(std::move(name_to_type_))
    , final_project(final_project_)
    , index_project(index_project_)
{
    for (const auto & item : assignments)
    {
        if (unlikely(!name_to_type[item.first]))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ProjectionStep miss type info for column " + item.first);
        output_stream->header.insert(ColumnWithTypeAndName{name_to_type[item.first], item.first});
    }
}

void ProjectionStepExt::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    // TODO: implement
    // auto actions = createActions(settings.context);
    // auto expression = std::make_shared<ExpressionActions>(actions, settings.getActionsSettings());

    // pipeline.addSimpleTransform([&](const Block & header) { return std::make_shared<ExpressionTransform>(header, expression); });
    // projection(pipeline, output_stream->header, settings);
}

std::shared_ptr<IQueryPlanStep> ProjectionStepExt::copy(ContextPtr) const
{
    return std::make_shared<ProjectionStepExt>(input_streams[0], assignments.copy(), name_to_type, final_project, index_project);
}

ActionsDAGPtr ProjectionStepExt::createActions(ContextPtr context) const
{
    ASTPtr expr_list = std::make_shared<ASTExpressionList>();

    NamesWithAliases output;
    for (const auto & item : assignments)
    {
        expr_list->children.emplace_back(item.second->clone());
        output.emplace_back(NameWithAlias{item.second->getColumnName(), item.first});
    }
    // TODO: implement
    // return createExpressionActions(context, input_streams[0].header.getNamesAndTypesList(), output, expr_list);
    return nullptr;
}

ActionsDAGPtr ProjectionStepExt::createActions(const Assignments & assignments, const NamesAndTypesList & source, ContextPtr context)
{
    ASTPtr expr_list = std::make_shared<ASTExpressionList>();

    NamesWithAliases output;
    for (const auto & item : assignments)
    {
        expr_list->children.emplace_back(item.second->clone());
        output.emplace_back(NameWithAlias{item.second->getColumnName(), item.first});
    }
    // TODO: implement
    // return createExpressionActions(context, source, output, expr_list);
    return nullptr;
}

// TODO: implement
// void ProjectionStepExt::prepare(const PreparedStatementContext & prepared_context)
// {
//     for (auto & assign : assignments)
//         prepared_context.prepare(assign.second);
// }

}
