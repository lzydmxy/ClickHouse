#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Core/Settings.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>

#include <Query/Parsers/ASTFieldReferenceExt.h>

#include <boost/algorithm/string/case_conv.hpp>

namespace DB
{

using DB::IAST;
using DB::ASTPtr;
using DB::ASTs;
using ConstASTPtr = std::shared_ptr<const IAST>;
using ConstASTs = std::vector<ConstASTPtr>;
using DB::Exception;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

#define APPLY_AST_TYPES(M) \
    M(ASTSetQuery) \
    M(ASTUseQuery) \
    M(ASTWithElement) \
    M(ASTArrayJoin) \
    M(ASTTableExpression) \
    M(ASTTableJoin) \
    M(ASTTablesInSelectQuery) \
    M(ASTTablesInSelectQueryElement) \
    M(ASTWindowDefinition) \
    M(ASTWindowListElement) \
    M(ASTFieldReferenceExt) \
    M(ASTColumnsApplyTransformer) \
    M(ASTColumnsExceptTransformer) \
    M(ASTColumnsReplaceTransformer) \
    M(ASTDictionary)
#define ENUM_TYPE(ITEM) ITEM,

enum class ASTType : UInt8
{
    APPLY_AST_TYPES(ENUM_TYPE) UNDEFINED,
};

#undef ENUM_TYPE

inline String toString(ASTType type)
{
    switch (type)
    {
#define ENUM_TYPE(ITEM) \
    case ASTType::ITEM: \
        return #ITEM;
        APPLY_AST_TYPES(ENUM_TYPE)
#undef ENUM_TYPE
        default:
            return "UNDEFINED";
    }
}

ASTType getAstType(ASTPtr & ast)
{
    if (auto * ast_use_query = ast->as<const ASTUseQuery>())
    {
        return ASTType::ASTUseQuery;  
    }
    else if (auto * ast_set_query = ast->as<const ASTSetQuery>())
    {
        return ASTType::ASTSetQuery;  
    }
    else if (auto *  ast_with_element = ast->as<const ASTWithElement>())
    {
        return ASTType::ASTWithElement;
    }
    else if (auto * ast_array_join = ast->as<const ASTArrayJoin>())
    {
         return ASTType::ASTArrayJoin;
    }
    else if (auto * ast_table_expression = ast->as<const ASTTableExpression>())
    {
        return ASTType::ASTTableExpression;
    }
    else if (auto * ast_table_join = ast->as<const ASTTableJoin>())
    {
        return ASTType::ASTTableJoin;
    }
    else if (auto * ast_tables_in_select_query = ast->as<const ASTTablesInSelectQuery>())
    {
        return ASTType::ASTTablesInSelectQuery;
    }
    else if (auto * ast_tables_in_select_query_element = ast->as<const ASTTablesInSelectQueryElement>())
    {
        return ASTType::ASTTablesInSelectQueryElement;
    }
    else if (auto * ast_window_definition = ast->as<const ASTWindowDefinition>())
    {
         return ASTType::ASTWindowDefinition;
    }
    else if (auto * ast_window_list_element = ast->as<const ASTWindowListElement>())
    {
        return ASTType::ASTWindowListElement;
    }
    else if (auto * ast_field_ref = ast->as<const ASTFieldReferenceExt>())
    {
        return ASTType::ASTFieldReferenceExt;
    }
    else if (auto * ast_columns_apply = ast->as<const ASTColumnsApplyTransformer>())
    {
        return ASTType::ASTColumnsApplyTransformer;
    }
    else if (auto * ast_columns_except = ast->as<const ASTColumnsExceptTransformer>())
    {
        return ASTType::ASTColumnsExceptTransformer;
    }
    else if (auto * ast_columns_replace = ast->as<const ASTColumnsReplaceTransformer>())
    {
        return ASTType::ASTColumnsReplaceTransformer;
    }
    //type not ASTSetQuery, need to continue

    return ASTType::UNDEFINED;
}

void astToLowerCase(ASTPtr & ast)
{
    if ( auto astColumnDeclaration = std::dynamic_pointer_cast<ASTColumnDeclaration>(ast) )
    {
        boost::to_lower(astColumnDeclaration->name);
    }

    //type not ASTColumnDeclaration, need to continue
    return;
}

void astToUpperCase(ASTPtr & ast)
{
    if ( auto astColumnDeclaration = std::dynamic_pointer_cast<ASTColumnDeclaration>(ast) )
    {
        boost::to_upper(astColumnDeclaration->name);
    }

    //type not ASTColumnDeclaration, need to continue
    return;
}

void setOrReplaceAST(ASTPtr & cur_ast, ASTPtr & old_ast, const ASTPtr & new_ast)
{
    if (!new_ast)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to set or replace AST subtree with nullptr");

    if (old_ast == new_ast)
        return;

    /// set ast
    if (!old_ast)
    {
        old_ast = new_ast;
        cur_ast->children.push_back(old_ast);
        return;
    }

    /// replace ast
    for (auto & current_child: cur_ast->children)
    {
        if (current_child == old_ast)
        {
            current_child = new_ast;
            old_ast = new_ast;
            return;
        }
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "AST subtree not found in children");
}

}

