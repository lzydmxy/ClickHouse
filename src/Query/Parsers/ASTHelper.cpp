#include <Query/Parsers/ASTHelper.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <Query/ProtosHelper/PlanSerDerHelper.h>
#include <Query/ProtosHelper/ASTSerDerHelper.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
}


void astToLowerCase(const ASTPtr & ast)
{
    if (auto * casted_ast = ast->as<ASTConstraintDeclaration>())
    {
        boost::to_lower(casted_ast->name);
    }
    else if (auto * casted_ast = ast->as<ASTPartitionExt>())
    {
        boost::to_lower(casted_ast->fields_str);
    }
    else if (auto * casted_ast = ast->as<ASTProjectionDeclaration>())
    {
        boost::to_lower(casted_ast->name);
    }
    else if (auto casted_ast = ast->as<ASTTableColumnReference>())
    {
        boost::to_lower(casted_ast->column_name);
    }
    else if (auto casted_ast = ast->as<ASTQuantifiedComparisonExt>())
    {
        boost::to_lower(casted_ast->alias);
        boost::to_lower(casted_ast->comparator);
    }

    // TODO wujianchao add more types
}

void astToUpperCase(const ASTPtr & ast)
{
    if (auto * casted_ast = ast->as<ASTConstraintDeclaration>())
    {
        boost::to_upper(casted_ast->name);
    }
    else if (auto * casted_ast = ast->as<ASTPartitionExt>())
    {
        boost::to_upper(casted_ast->fields_str);
    }
    else if ( auto casted_ast = ast->as<ASTProjectionDeclaration>())
    {
        boost::to_upper(casted_ast->name);
    }
    else if (auto casted_ast = ast->as<ASTTableColumnReference>())
    {
        boost::to_upper(casted_ast->column_name);
    }
    else if (auto casted_ast = ast->as<ASTQuantifiedComparisonExt>())
    {
        boost::to_upper(casted_ast->alias);
        boost::to_upper(casted_ast->comparator);
    }

    // TODO wujianchao add more types
}

void setOrReplaceAST(ASTPtr & cur_ast, ASTPtr & old_child, const ASTPtr & new_child)
{
    if (!new_child)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to set or replace AST subtree with nullptr");

    if (old_child == new_child)
        return;

    /// set ast
    if (!old_child)
    {
        old_child = new_child;
        cur_ast->children.push_back(old_child);
        return;
    }

    /// replace ast
    for (auto & current_child: cur_ast->children)
    {
        if (current_child == old_child)
        {
            current_child = new_child;
            old_child = new_child;
            return;
        }
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "AST subtree not found in children");
}

[[ noreturn ]] void serializeASTImpl(const ConstASTPtr & ast, WriteBuffer & buf)
{
    serializeASTImpl(*ast, buf);
}

[[ noreturn ]] void serializeASTImpl(const IAST & ast, WriteBuffer & buf)
{
    if (const auto * casted = ast.as<ASTArrayJoin>())
    {
        serializeEnum(casted->kind, buf);
        serializeAST(casted->expression_list, buf);
    }
    // todo wujianchao add more types
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implement serialize of {}", toString(getAstType(ast)));
}

ASTPtr deserializeASTImpl(ASTType type, ReadBuffer & buf)
{
    switch (type)
    {
        case ASTType::ASTArrayJoin:
        {
            auto ast = std::make_shared<ASTArrayJoin>();
            deserializeEnum(ast->kind, buf);
            ast->expression_list = deserializeASTWithChildren(ast->children, buf);
            return ast;
        }
        // todo wujianchao add more types
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implement deserializeASTImpl AST for {}", toString(type));
    }
}

ASTFunctionPtr makeASTFunctionWithVectorArgs(ASTFunctionPtr & ast, const String &name, ASTs &&args)
{
    auto function = std::make_shared<ASTFunction>();
    ast->name = name;
    ast->arguments = std::make_shared<ASTExpressionList>();
    ast->children.push_back(function->arguments);
    ast->arguments->children = std::move(args);

    return function;
}

}

