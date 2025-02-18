#pragma once

#include <Parsers/ASTSelectQuery.h>

namespace DB
{

/** SELECT query
  */
class ASTSelectQueryExt : public ASTSelectQuery
{
public:
    static void collectAllTables(const IAST * ast, std::vector<ASTPtr> &, bool &);

    ASTPtr & refGroupBy() { return getExpression(Expression::GROUP_BY); }
    ASTPtr & refWindow() { return getExpression(Expression::WINDOW); }
    ASTPtr & refOrderBy() { return getExpression(Expression::ORDER_BY); }
    ASTPtr & refLimitLength() { return getExpression(Expression::LIMIT_LENGTH); }

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

    std::vector<Expression> getExpressionTypes() const;
    void removeSettingsAndOutputFormat();
};

}
