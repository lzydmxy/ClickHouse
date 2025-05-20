#pragma once

#include <Query/Optimizer/SimpleExpressionRewriter.h>
#include <Query/Optimizer/Utils.h>
#include <Query/Parsers/ASTVisitor.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
class FunctionExtractorVisitor : public ConstASTVisitor<Void, std::set<String>>
{
public:
    Void visitNode(const ConstASTPtr &, std::set<String> & context) override;
    Void visitASTFunction(const ConstASTPtr &, std::set<String> & context) override;
};

class FunctionExtractor
{
public:
    static std::set<String> extract(ConstASTPtr node)
    {
        FunctionExtractorVisitor visitor;
        std::set<String> context;
        ASTVisitorUtil::accept(node, visitor, context);
        return context;
    }
};

}
