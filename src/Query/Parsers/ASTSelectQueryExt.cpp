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

ASTPtr ASTSelectQueryExt::clone() const
{
    auto res = std::make_shared<ASTSelectQueryExt>(*this);

    /** NOTE Members must clone exactly in the same order in which they were inserted into `children` in ParserSelectQuery.
     * This is important because the AST hash depends on the children order and this hash is used for multiple things,
     * like the column identifiers in the case of subqueries in the IN statement or caching scalar queries (reused in CTEs so it's
     * important for them to have the same hash).
     * For distributed query processing, in case one of the servers is localhost and the other one is not, localhost query is executed
     * within the process and is cloned, and the request is sent to the remote server in text form via TCP.
     * And if the cloning order does not match the parsing order then different servers will get different identifiers.
     *
     * Since the positions map uses <key, position> we can copy it as is and ensure the new children array is created / pushed
     * in the same order as the existing one */
    res->children.clear();
    for (const auto & child : children)
        res->children.push_back(child->clone());

    return res;
}

}
