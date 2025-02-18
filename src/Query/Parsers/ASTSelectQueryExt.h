#pragma once

#include <Parsers/ASTSelectQuery.h>

namespace DB
{

/** SELECT query
  */
class ASTSelectQueryExt : public ASTSelectQuery
{
public:
    enum class Expression : uint8_t
    {
        WITH,
        SELECT,
        TABLES,
        PREWHERE,
        WHERE,
        GROUP_BY,
        HAVING,
        WINDOW,
        ORDER_BY,
        LIMIT_BY_OFFSET,
        LIMIT_BY_LENGTH,
        LIMIT_BY,
        LIMIT_OFFSET,
        LIMIT_LENGTH,
        SETTINGS,
        ESCAPE
    };

    static String expressionToString(Expression expr)
    {
        switch (expr)
        {
            case Expression::WITH:
                return "WITH";
            case Expression::SELECT:
                return "SELECT";
            case Expression::TABLES:
                return "TABLES";
            case Expression::PREWHERE:
                return "PREWHERE";
            case Expression::WHERE:
                return "WHERE";
            case Expression::GROUP_BY:
                return "GROUP BY";
            case Expression::HAVING:
                return "HAVING";
            case Expression::WINDOW:
                return "WINDOW";
            case Expression::ORDER_BY:
                return "ORDER BY";
            case Expression::LIMIT_BY_OFFSET:
                return "LIMIT BY OFFSET";
            case Expression::LIMIT_BY_LENGTH:
                return "LIMIT BY LENGTH";
            case Expression::LIMIT_BY:
                return "LIMIT BY";
            case Expression::LIMIT_OFFSET:
                return "LIMIT OFFSET";
            case Expression::LIMIT_LENGTH:
                return "LIMIT LENGTH";
            case Expression::SETTINGS:
                return "SETTINGS";
            case Expression::ESCAPE:
                return "ESCAPE";
        }
        return "";
    }

    ASTPtr clone() const override;

    static void collectAllTables(const IAST * ast, std::vector<ASTPtr> &, bool &);

    ASTPtr & refGroupBy() { return getExpression(Expression::GROUP_BY); }
    ASTPtr & refWindow() { return getExpression(Expression::WINDOW); }
    ASTPtr & refOrderBy() { return getExpression(Expression::ORDER_BY); }
    ASTPtr & refLimitLength() { return getExpression(Expression::LIMIT_LENGTH); }

    ASTPtr escape() const { return getExpression(Expression::ESCAPE); }

    ASTPtr getWith() const { return getExpression(Expression::WITH, true); }
    ASTPtr getSelect() const { return getExpression(Expression::SELECT, true); }
    ASTPtr getTables() const { return getExpression(Expression::TABLES, true); }
    ASTPtr getPrewhere() const { return getExpression(Expression::PREWHERE, true); }
    ASTPtr getWhere() const { return getExpression(Expression::WHERE, true); }
    ASTPtr getGroupBy() const { return getExpression(Expression::GROUP_BY, true); }
    ASTPtr getHaving() const { return getExpression(Expression::HAVING, true); }
    ASTPtr getWindow() const { return getExpression(Expression::WINDOW, true); }
    ASTPtr getOrderBy() const { return getExpression(Expression::ORDER_BY, true); }
    ASTPtr getLimitByOffset() const { return getExpression(Expression::LIMIT_BY_OFFSET, true); }
    ASTPtr getLimitByLength() const { return getExpression(Expression::LIMIT_BY_LENGTH, true); }
    ASTPtr getLimitBy() const { return getExpression(Expression::LIMIT_BY, true); }
    ASTPtr getLimitOffset() const { return getExpression(Expression::LIMIT_OFFSET, true); }
    ASTPtr getLimitLength() const { return getExpression(Expression::LIMIT_LENGTH, true); }
    ASTPtr getSettings() const { return getExpression(Expression::SETTINGS, true); }

    /// Set/Reset/Remove expression.
    void setExpression(Expression expr, ASTPtr && ast);
    ASTPtr getExpression(Expression expr, bool clone = false) const
    {
        auto it = positions.find(expr);
        if (it != positions.end())
            return clone ? children[it->second]->clone() : children[it->second];
        return {};
    }
    std::vector<Expression> getExpressionTypes() const;
    void removeSettingsAndOutputFormat();

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

private:
    std::unordered_map<Expression, size_t> positions;

    ASTPtr & getExpression(Expression expr);
};

}
