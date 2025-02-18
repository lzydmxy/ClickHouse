#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Core/Settings.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <Parsers/ASTFunctionWithKeyValueArguments.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Parsers/ASTProjectionSelectQuery.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSampleRatio.h>
#include <Query/Parsers/ASTSelectQueryExt.h>
#include <Parsers/ASTTTLElement.h>
#include <Parsers/Access/ASTRowPolicyName.h>
#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <Query/Parsers/ASTPartitionExt.h>

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
    M(ASTDictionary) \
    M(ASTPair) \
    M(ASTPartitionExt) \
    M(ASTProjectionDeclaration) \
    M(ASTProjectionSelectQuery) \
    M(ASTQualifiedAsterisk) \
    M(ASTRowPolicyName) \
    M(ASTRowPolicyNames) \
    M(ASTSampleRatio) \
    M(ASTSelectQueryExt) \
    M(ASTSetQuery) \
    M(ASTSettingsProfileElement) \
    M(ASTSettingsProfileElements) \
    M(ASTTTLElement)
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
    if ( auto astSetQuery = std::dynamic_pointer_cast<ASTSetQuery>(ast) )
    {
        return ASTType::ASTDictionary;  
    }
    else if ( auto astPair = std::dynamic_pointer_cast<ASTPair>(ast))
    {
        return ASTType::ASTPair;
    }
    else if ( auto astPartitionExt = std::dynamic_pointer_cast<ASTPartitionExt>(ast))
    {
        return ASTType::ASTPartitionExt;
    }
    else if ( auto astProjectionDeclaration = std::dynamic_pointer_cast<ASTProjectionDeclaration>(ast))
    {
        return ASTType::ASTProjectionDeclaration;
    }
    else if ( auto astProjectionSelectQuery = std::dynamic_pointer_cast<ASTProjectionSelectQuery>(ast))
    {
        return ASTType::ASTProjectionSelectQuery;
    }
    else if ( auto astQualifiedAsterisk = std::dynamic_pointer_cast<ASTQualifiedAsterisk>(ast))
    {
        return ASTType::ASTQualifiedAsterisk;
    }
    else if ( auto astRowPolicyName = std::dynamic_pointer_cast<ASTRowPolicyName>(ast))
    {
        return ASTType::ASTRowPolicyName;
    }
    else if ( auto astRowPolicyNames = std::dynamic_pointer_cast<ASTRowPolicyNames>(ast))
    {
        return ASTType::ASTRowPolicyNames;
    }
    else if ( auto astSampleRatio = std::dynamic_pointer_cast<ASTSampleRatio>(ast))
    {
        return ASTType::ASTSampleRatio;
    }
    else if ( auto astSelectQueryExt = std::dynamic_pointer_cast<ASTSelectQueryExt>(ast))
    {
        return ASTType::ASTSelectQueryExt;
    }
    else if ( auto astSetQuery = std::dynamic_pointer_cast<ASTSetQuery>(ast))
    {
        return ASTType::ASTSetQuery;
    }
    else if ( auto astSettingsProfileElement = std::dynamic_pointer_cast<ASTSettingsProfileElement>(ast))
    {
        return ASTType::ASTSettingsProfileElement;
    }
    else if ( auto astSettingsProfileElements = std::dynamic_pointer_cast<ASTSettingsProfileElements>(ast))
    {
        return ASTType::ASTSettingsProfileElements;
    }
    else if ( auto astTTLElement = std::dynamic_pointer_cast<ASTTTLElement>(ast))
    {
        return ASTType::ASTTTLElement;
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
    else if ( auto astPartitionExt = std::dynamic_pointer_cast<ASTPartitionExt>(ast))
    {
        boost::to_lower(astPartitionExt->fields_str);
    }
    else if ( auto astProjectionDeclaration = std::dynamic_pointer_cast<ASTProjectionDeclaration>(ast))
    {
        boost::to_lower(astProjectionDeclaration->name);
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
    else if ( auto astPartitionExt = std::dynamic_pointer_cast<ASTPartitionExt>(ast))
    {
        boost::to_upper(astPartitionExt->fields_str);
    }
    else if ( auto astProjectionDeclaration = std::dynamic_pointer_cast<ASTProjectionDeclaration>(ast))
    {
        boost::to_upper(astProjectionDeclaration->name);
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

