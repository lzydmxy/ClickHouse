#pragma once

#include <Query/Parsers/ASTVisitor.h>
#include <Parsers/IAST_fwd.h>
#include <Query/Optimizer/SimpleExpressionRewriter.h>
#include <Query/Processors/QueryPlan/Assignment.h>
#include <unordered_map>

namespace DB
{
class ExpressionInliner
{
public:
    static ASTPtr inlineSymbols(const ConstASTPtr & predicate, const Assignments & mapping);
};

class InlinerVisitor : public SimpleExpressionRewriter<const Assignments>
{
public:
    ASTPtr visitASTIdentifier(ASTPtr & node, const Assignments & context) override;
};

}
