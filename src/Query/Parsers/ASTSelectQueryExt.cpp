#include <queue>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Query/Parsers/ASTSelectQueryExt.h>

namespace DB
{

ASTPtr ASTSelectQueryExt::clone() const
{
    auto res = std::make_shared<ASTSelectQueryExt>(*this);
    res->children.clear();
    res->positions.clear();

#define CLONE(expr) res->setExpression(expr, getExpression(expr, true))

    /** NOTE Members must clone exactly in the same order,
        *  in which they were inserted into `children` in ParserSelectQuery.
        * This is important because of the children's names the identifier (getTreeHash) is compiled,
        *  which can be used for column identifiers in the case of subqueries in the IN statement.
        * For distributed query processing, in case one of the servers is localhost and the other one is not,
        *  localhost query is executed within the process and is cloned,
        *  and the request is sent to the remote server in text form via TCP.
        * And if the cloning order does not match the parsing order,
        *  then different servers will get different identifiers.
        */
    CLONE(Expression::WITH);
    CLONE(Expression::SELECT);
    CLONE(Expression::TABLES);
    CLONE(Expression::PREWHERE);
    CLONE(Expression::WHERE);
    CLONE(Expression::GROUP_BY);
    CLONE(Expression::HAVING);
    CLONE(Expression::WINDOW);
    CLONE(Expression::ORDER_BY);
    CLONE(Expression::LIMIT_BY_OFFSET);
    CLONE(Expression::LIMIT_BY_LENGTH);
    CLONE(Expression::LIMIT_BY);
    CLONE(Expression::LIMIT_OFFSET);
    CLONE(Expression::LIMIT_LENGTH);
    CLONE(Expression::SETTINGS);

#undef CLONE

    return res;
}

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

void ASTSelectQueryExt::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    frame.current_select = this;
    frame.need_parens = false;
    frame.expression_list_prepend_whitespace = true;

    std::string indent_str = s.one_line ? "" : std::string(4 * frame.indent, ' ');

    if (with())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << indent_str << "WITH" << (s.hilite ? hilite_none : "");
        s.one_line ? with()->formatImpl(s, state, frame) : with()->as<ASTExpressionList &>().formatImplMultiline(s, state, frame);
        s.ostr << s.nl_or_ws;
    }

    s.ostr << (s.hilite ? hilite_keyword : "") << indent_str << "SELECT" << (distinct ? " DISTINCT" : "") << (s.hilite ? hilite_none : "");

    s.one_line ? select()->formatImpl(s, state, frame) : select()->as<ASTExpressionList &>().formatImplMultiline(s, state, frame);

    if (tables())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "FROM" << (s.hilite ? hilite_none : "");
        tables()->formatImpl(s, state, frame);
    }

    if (prewhere())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "PREWHERE " << (s.hilite ? hilite_none : "");
        prewhere()->formatImpl(s, state, frame);
    }

    if (where())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "WHERE " << (s.hilite ? hilite_none : "");
        where()->formatImpl(s, state, frame);
    }

    if (escape())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "ESCAPE " << (s.hilite ? hilite_none : "");
        escape()->formatImpl(s, state, frame);
    }

    if (!group_by_all && groupBy())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "GROUP BY" << (s.hilite ? hilite_none : "");
        if (!group_by_with_grouping_sets)
            s.one_line ? groupBy()->formatImpl(s, state, frame) : groupBy()->as<ASTExpressionList &>().formatImplMultiline(s, state, frame);
    }

    if (group_by_all)
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "GROUP BY ALL" << (s.hilite ? hilite_none : "");

    if (group_by_with_rollup)
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << (s.one_line ? "" : "    ") << "WITH ROLLUP"
               << (s.hilite ? hilite_none : "");

    if (group_by_with_cube)
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << (s.one_line ? "" : "    ") << "WITH CUBE"
               << (s.hilite ? hilite_none : "");

    if (group_by_with_grouping_sets)
    {
        frame.surround_each_list_element_with_parens = true;
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << (s.one_line ? "" : "    ") << "GROUPING SETS"
               << (s.hilite ? hilite_none : "");
        s.ostr << " (";
        s.one_line ? groupBy()->formatImpl(s, state, frame) : groupBy()->as<ASTExpressionList &>().formatImplMultiline(s, state, frame);
        s.ostr << ")";
        frame.surround_each_list_element_with_parens = false;
    }

    if (group_by_with_totals)
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << (s.one_line ? "" : "    ") << "WITH TOTALS"
               << (s.hilite ? hilite_none : "");

    if (having())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "HAVING " << (s.hilite ? hilite_none : "");
        having()->formatImpl(s, state, frame);
    }

    if (window())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "WINDOW" << (s.hilite ? hilite_none : "");
        window()->as<ASTExpressionList &>().formatImplMultiline(s, state, frame);
    }

    if (!order_by_all && orderBy())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "ORDER BY" << (s.hilite ? hilite_none : "");
        s.one_line ? orderBy()->formatImpl(s, state, frame) : orderBy()->as<ASTExpressionList &>().formatImplMultiline(s, state, frame);
    }

    if (order_by_all)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "ORDER BY " << (s.hilite ? hilite_none : "");

        auto * elem = orderBy()->children[0]->as<ASTOrderByElement>();
        elem->children.front()->formatImpl(s, state, frame);
        s.ostr << (s.hilite ? hilite_keyword : "") << (elem->direction == -1 ? " DESC" : " ASC") << (s.hilite ? hilite_none : "");

        if (elem->nulls_direction_was_explicitly_specified)
        {
            s.ostr << (s.hilite ? hilite_keyword : "") << " NULLS " << (elem->nulls_direction == elem->direction ? "LAST" : "FIRST")
                   << (s.hilite ? hilite_none : "");
        }
    }

    if (limitByLength())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "LIMIT " << (s.hilite ? hilite_none : "");
        if (limitByOffset())
        {
            limitByOffset()->formatImpl(s, state, frame);
            s.ostr << ", ";
        }
        limitByLength()->formatImpl(s, state, frame);
        s.ostr << (s.hilite ? hilite_keyword : "") << " BY" << (s.hilite ? hilite_none : "");
        s.one_line ? limitBy()->formatImpl(s, state, frame) : limitBy()->as<ASTExpressionList &>().formatImplMultiline(s, state, frame);
    }

    if (limitLength())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "LIMIT " << (s.hilite ? hilite_none : "");
        if (limitOffset())
        {
            limitOffset()->formatImpl(s, state, frame);
            s.ostr << ", ";
        }
        limitLength()->formatImpl(s, state, frame);
        if (limit_with_ties)
            s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << " WITH TIES" << (s.hilite ? hilite_none : "");
    }
    else if (limitOffset())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "OFFSET " << (s.hilite ? hilite_none : "");
        limitOffset()->formatImpl(s, state, frame);
    }

    if (settings())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "SETTINGS " << (s.hilite ? hilite_none : "");
        settings()->formatImpl(s, state, frame);
    }
}

void ASTSelectQueryExt::setExpression(Expression expr, ASTPtr && ast)
{
    if (ast)
    {
        auto it = positions.find(expr);
        if (it == positions.end())
        {
            positions[expr] = children.size();
            children.emplace_back(ast);
        }
        else
            children[it->second] = ast;
    }
    else if (positions.contains(expr))
    {
        size_t pos = positions[expr];
        children.erase(children.begin() + pos);
        positions.erase(expr);
        for (auto & pr : positions)
            if (pr.second > pos)
                --pr.second;
    }
}

ASTPtr & ASTSelectQueryExt::getExpression(Expression expr)
{
    if (!positions.contains(expr))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Get expression before set");
    return children[positions[expr]];
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
