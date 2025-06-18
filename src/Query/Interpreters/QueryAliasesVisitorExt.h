#pragma once

#include <Interpreters/Aliases.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTSelectQuery;
class ASTSubquery;
struct ASTTableExpression;
struct ASTArrayJoin;

template <bool allow_ambiguous_>
struct QueryAliasesWithSubqueriesExt
{
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);
    static constexpr bool allow_ambiguous = allow_ambiguous_;
};

template <bool allow_ambiguous_>
struct QueryAliasesNoSubqueriesExt
{
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);
    static constexpr bool allow_ambiguous = allow_ambiguous_;
};

/// Visits AST node to collect aliases.
template <typename Helper>
class QueryAliasesMatcherExt
{
public:
    using Visitor = ConstInDepthNodeVisitor<QueryAliasesMatcherExt, false>;

    using Data = Aliases;

    static void visit(const ASTPtr & ast, Data & data);
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child) { return Helper::needChildVisit(node, child); }

private:
    static void visit(const ASTSelectQuery & select, const ASTPtr & ast, Data & data);
    static void visit(const ASTSubquery & subquery, const ASTPtr & ast, Data & data);
    static void visit(const ASTArrayJoin &, const ASTPtr & ast, Data & data);
    static void visitOther(const ASTPtr & ast, Data & data);
};

using QueryAliasesAllowAmbiguousVisitor = QueryAliasesMatcherExt<QueryAliasesWithSubqueriesExt<true>>::Visitor;
using QueryAliasesAllowAmbiguousNoSubqueriesVisitor = QueryAliasesMatcherExt<QueryAliasesNoSubqueriesExt<true>>::Visitor;
}
