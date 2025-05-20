#pragma once

#include <Query/Analyzer/TypeAnalyzer.h>
#include <Interpreters/Context_fwd.h>
#include <Query/Optimizer/SimpleExpressionRewriter.h>
#include <Query/Optimizer/EqualityASTMap.h>
#include <Query/Parsers/ASTVisitor.h>
#include <Parsers/IAST_fwd.h>

#include <unordered_map>

namespace DB
{
using ConstASTMap = EqualityASTMap<ConstHashAST>;

class ExpressionRewriter
{
public:
    static ASTPtr rewrite(const ConstASTPtr & expression, ConstASTMap & expression_map);
};

class ExpressionRewriterVisitor : public SimpleExpressionRewriter<ConstASTMap>
{
public:
    ASTPtr visitNode(ASTPtr & node, ConstASTMap & expression_map) override;
};

class FunctionIsInjective
{
public:
    static bool isInjective(const ConstASTPtr & expr, ContextMutablePtr & context, const NamesAndTypes & input_types, const NameSet & partition_cols);
};

class FunctionIsInjectiveVisitor : public ConstASTVisitor<bool, NameSet>
{
public:
    FunctionIsInjectiveVisitor(ContextMutablePtr & context_, const std::unordered_map<ASTPtr, ColumnWithType> & expr_types_)
        : context(context_), expr_types(expr_types_)
    {
    }
    bool visitNode(const ConstASTPtr &, NameSet & context) override;
    bool visitASTFunction(const ConstASTPtr &, NameSet & context) override;
    bool visitASTIdentifier(const ConstASTPtr &, NameSet & context) override;

private:
    ContextMutablePtr & context;
    std::unordered_map<ASTPtr, ColumnWithType> expr_types;
};
}
