#pragma once

#include <memory>
#include <arrow/compute/expression.h>
#include <capnp/compiler/grammar.capnp.h>
#include <Query/Optimizer/SimpleExpressionRewriter.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
class SQLFingerprintRewriter : public SimpleExpressionRewriter<Void>
{
public:
    static void rewriteAST(ASTPtr & node)
    {
        SQLFingerprintRewriter rewriter;
        Void dummy_context;
        node = rewriter.visitNode(node, dummy_context);
        ASTQueryWithOutput::resetOutputASTIfExist(*node);
    }

    ASTPtr visitASTLiteral(ASTPtr & node, Void & context) override
    {
        auto literal_ptr = std::dynamic_pointer_cast<ASTLiteral>(node);
        literal_ptr->value = Field("?");
        return visitNode(node, context);
    }

    ASTPtr visitASTTableIdentifier(ASTPtr & node, Void & context) override
    {
        auto literal_ptr = std::dynamic_pointer_cast<ASTTableIdentifier>(node);
        literal_ptr->full_name = "?";
        literal_ptr->uuid = UUIDHelpers::Nil;
        return visitNode(node, context);
    }

    ASTPtr visitASTSelectQuery(ASTPtr & node, Void & context) override
    {
        auto select_ptr = std::dynamic_pointer_cast<ASTSelectQuery>(node);
        select_ptr->removeSettingsAndOutputFormat();
        return visitNode(node, context);
    }

};
}
