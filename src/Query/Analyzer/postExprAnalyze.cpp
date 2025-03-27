#include <Query/Analyzer/postExprAnalyze.h>

#include <DataTypes/DataTypeMap.h>
#include <Query/Common/MapHelpers.h>
#include <Common/FieldVisitorToString.h>


namespace DB
{

void postExprAnalyze(ASTFunctionPtr & function, const ColumnsWithTypeAndName & processed_arguments, Analysis & analysis, ContextPtr context)
{
    String func_name_lowercase = Poco::toLower(function->name);

    auto check_origin_column = [](const FieldDescription & field, auto && pred) {
        if (!field.hasOriginInfo())
            return false;

        for (const auto & origin_col : field.origin_columns)
            if (!pred(origin_col))
                return false;

        return true;
    };

    auto register_subcolumn = [&](const ASTPtr & ast, const ResolvedField & column_ref, const SubColumnID & sub_column_id) {
        analysis.setSubColumnReference(ast, SubColumnReference{column_ref, sub_column_id});

        for (const auto & origin_col : column_ref.getFieldDescription().origin_columns)
            analysis.addReadSubColumn(origin_col.table_ast, origin_col.index_of_scope, sub_column_id);
    };

    do
    {
        // TODO: if support FunctionMapElement, then add other code

        // TODO: if support FunctionMapKeys, then add other code

        // TODO: if support FunctionMapValues, then add other code

        if ((func_name_lowercase == "get_json_object" || func_name_lowercase == "jsonextractraw")
            && context->getOptimizerContext()->getSettingsRef().optimize_json_function_to_subcolumn)
        {
            auto column_reference = analysis.tryGetColumnReference(function->arguments->children[0]);
            if (!column_reference)
                break;

            const auto & resolved_field = column_reference->getFieldDescription();

            String column_name;
            if (processed_arguments[1].column)
            {
                column_name = (*processed_arguments[1].column)[0].safeGet<String>();
                if (func_name_lowercase == "get_json_object" && startsWith(column_name, "$."))
                    column_name = column_name.substr(2, column_name.length() - 2);
            }
            if (column_name.empty())
                break;

            auto column_id = SubColumnID::jsonField(column_name);

            if (!check_origin_column(resolved_field, [&](const auto & origin) -> bool {
                    auto storage_snapshot = origin.storage->getStorageSnapshot(origin.metadata_snapshot, context);
                    GetColumnsOptions options{GetColumnsOptions::All};
                    options.withSubcolumns();
                    options.withExtendedObjects();
                    return /* context->getSettingsRef().allow_nonexist_object_subcolumns || */
                        !!(storage_snapshot->tryGetColumn(options, column_id.getSubColumnName(origin.column)));
                }))
                break;

            register_subcolumn(function, *column_reference, column_id);
        }
    } while (false);
}

}
