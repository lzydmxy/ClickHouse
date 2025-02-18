#include <Query/Parsers/ASTHelper.h>

#include <boost/algorithm/string/case_conv.hpp>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
}


void astToLowerCase(const ASTPtr & ast)
{
    if (auto * casted_ast = ast->as<ASTColumnDeclaration>())
    {
        boost::to_lower(casted_ast->name);
    }
    if (auto * casted_ast = ast->as<ASTConstraintDeclaration>())
    {
        boost::to_lower(casted_ast->name);
    }

    // TODO add more types
}

void astToUpperCase(const ASTPtr & ast)
{
    if (auto * casted_ast = ast->as<ASTColumnDeclaration>())
    {
        boost::to_upper(casted_ast->name);
    }
    if (auto * casted_ast = ast->as<ASTConstraintDeclaration>())
    {
        boost::to_upper(casted_ast->name);
    }

    // TODO add more types
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

}
