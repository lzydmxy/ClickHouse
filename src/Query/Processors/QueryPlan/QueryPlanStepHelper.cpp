#include <Query/Processors/QueryPlan/QueryPlanStepHelper.h>


namespace DB
{

ActionsDAGPtr QueryPlanStepHelper::createFilterExpressionActions(ContextPtr context, const ASTPtr & filter, const Block & header)
{
    Names output;
    for (const auto & item : header)
        output.emplace_back(item.name);
    output.push_back(filter->getColumnName());

    return createExpressionActions(context, header.getNamesAndTypesList(), output, filter);
}

ActionsDAGPtr QueryPlanStepHelper::createExpressionActions(
    ContextPtr context, const NamesAndTypesList & source, const NamesWithAliases & output, const ASTPtr & ast, bool add_project)
{
    PreparedSetsPtr prepared_sets;
    auto settings = context->getSettingsRef();
    SizeLimits size_limits_for_set(settings.max_rows_in_set, settings.max_bytes_in_set, settings.set_overflow_mode);
    auto actions = std::make_shared<ActionsDAG>(source);
    const NamesAndTypesList aggregation_keys;
    const ColumnNumbersList grouping_set_keys;
    ActionsVisitor::Data visitor_data(
        context,
        size_limits_for_set,
        0,
        source,
        std::move(actions),
        prepared_sets,
        true,
        false,
        false,
        {aggregation_keys, grouping_set_keys, GroupByKind::NONE});
    ActionsVisitor(visitor_data).visit(ast);
    actions = visitor_data.getActions();

    if (add_project)
        actions->project(output);
    else
        actions->addAliases(output);

    Names output_columns;
    for (const auto & item : output)
        if (!item.second.empty())
            output_columns.emplace_back(item.second);
        else
            output_columns.emplace_back(item.first);

    actions->removeUnusedActions(output_columns);

    return actions;
}

ActionsDAGPtr QueryPlanStepHelper::createExpressionActions(
    ContextPtr context, const NamesAndTypesList & source, const Names & output, const ASTPtr & ast, bool add_project)
{
    NamesWithAliases names_with_aliases;
    for (const auto & item : output)
        names_with_aliases.emplace_back(NameWithAlias{item, ""});

    return createExpressionActions(context, source, names_with_aliases, ast, add_project);
}

}
