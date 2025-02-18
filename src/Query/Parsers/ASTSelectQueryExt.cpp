#include <queue>

#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Query/Parsers/ASTSelectQueryExt.h>

namespace DB
{

void ASTSelectQueryExt::collectAllTables(const IAST * ast, std::vector<ASTPtr> & all_tables, bool & has_table_functions)
{
    if (!ast)
        return;
    // BFS ASTSelectQueryExt and get all Tables;
    std::queue<const IAST *> q;
    if (const auto * select = ast->as<ASTSelectQueryExt>())
    {
        q.push(select);
        while (!q.empty())
        {
            auto & n = q.front();
            for (const auto & c : n->children)
                q.push(c.get());

            if (const ASTTableExpression * tbl = typeid_cast<const ASTTableExpression *>(n))
            {
                if (tbl->database_and_table_name)
                    all_tables.push_back(tbl->database_and_table_name);
                else if (tbl->table_function)
                    has_table_functions = true;
            }

            q.pop();
        }
    }
    else if (const auto * select_union = ast->as<ASTSelectWithUnionQuery>())
    {
        for (const auto & child : select_union->list_of_selects->children)
            collectAllTables(child.get(), all_tables, has_table_functions);
    }
    else if (const auto * intersect_or_except = ast->as<ASTSelectIntersectExceptQuery>())
    {
        for (const auto & child : intersect_or_except->getListOfSelects())
            collectAllTables(child.get(), all_tables, has_table_functions);
    }
}

std::vector<ASTSelectQueryExt::Expression> ASTSelectQueryExt::getExpressionTypes() const
{
    std::vector<Expression> expression_types(positions.size());

    for (const auto & [type, index] : positions)
        expression_types[index] = type;

    return expression_types;
}

void ASTSelectQueryExt::removeSettingsAndOutputFormat()
{
    positions.erase(Expression::SETTINGS);
}

}
