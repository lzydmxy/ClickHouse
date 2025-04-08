#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSubquery.h>

namespace DB
{

struct ExecutePrewhereSubquery
{
    using Data = ExecutePrewhereSubquery;

    ContextMutablePtr context;
    explicit ExecutePrewhereSubquery(ContextMutablePtr context_) : context(std::move(context_)) {}

    static void visit(ASTPtr & ast, Data & impl)
    {
        if (auto * subquery = typeid_cast<ASTSubquery *>(ast.get()))
            impl.visit(*subquery, ast);
        else if (auto * function = typeid_cast<ASTFunction *>(ast.get()))
            impl.visit(*function, ast);
    }

    static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }

    void visit(ASTSubquery & subquery, ASTPtr & ast) const;
    void visit(ASTFunction & function, ASTPtr & ast) const;

    void rewriteSubqueryToScalarLiteral(ASTSubquery & subquery, ASTPtr & ast) const;
    bool rewriteSubqueryToSet(ASTSubquery & subquery, ASTPtr & ast) const;
};

using ExecutePrewhereSubqueryVisitor = InDepthNodeVisitor<ExecutePrewhereSubquery, true>;

}
