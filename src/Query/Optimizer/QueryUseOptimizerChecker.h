#pragma once

#include <Core/QueryProcessingStage.h>
#include <Query/Parsers/ASTVisitor.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
class Context;

void turnOffOptimizer(ContextMutablePtr context, ASTPtr & node);

class QueryUseOptimizerChecker
{
public:
    static bool check(ASTPtr node, ContextMutablePtr context, bool throw_exception = false);
};

struct QueryUseOptimizerContext
{
    ContextMutablePtr context;
    NameSet ctes = {};
    Tables external_tables = {};
    bool disallow_with_totals = false;
    bool disallow_subquery = false;
};

class QueryUseOptimizerVisitor : public ASTVisitor<bool, QueryUseOptimizerContext>
{
public:
    bool visitNode(ASTPtr & node, QueryUseOptimizerContext &) override;
    bool visitASTSelectQuery(ASTPtr & node, QueryUseOptimizerContext &) override;
    bool visitASTTableJoin(ASTPtr & node, QueryUseOptimizerContext &) override;
    bool visitASTIdentifier(ASTPtr & node, QueryUseOptimizerContext &) override;
    bool visitASTFunction(ASTPtr & node, QueryUseOptimizerContext &) override;
    bool visitASTQuantifiedComparisonExt(ASTPtr & node, QueryUseOptimizerContext &) override;
    const String & getReason() const { return reason; }

private:
    static void collectWithTableNames(ASTSelectQuery & query, NameSet & with_tables);
    String reason;
};

}
