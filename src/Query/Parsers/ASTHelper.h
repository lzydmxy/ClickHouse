#pragma once

#include <Parsers/IAST_fwd.h>
#include <Core/Settings.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>

namespace DB
{

using DB::IAST;
using DB::ASTPtr;
using DB::ASTs;
using ConstASTPtr = std::shared_ptr<const IAST>;
using ConstASTs = std::vector<ConstASTPtr>;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

#define APPLY_AST_TYPES(M) \
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
    auto astSetQuery = std::dynamic_pointer_cast<ASTSetQuery>(ast);
    if ( astSetQuery )
    {
        return ASTType::ASTDictionary;  
    }

    //type not ASTSetQuery, need to continue

    return ASTType::UNDEFINED;
}

void astToLowerCase(ASTPtr & ast)
{
    auto astColumnDeclaration = std::dynamic_pointer_cast<ASTColumnDeclaration>(ast);
    if ( astColumnDeclaration )
    {
        boost::to_lower(astColumnDeclaration.name);
    }

    //type not ASTColumnDeclaration, need to continue
    return;
}

void astToUpperCase(ASTPtr & ast)
{
    auto astColumnDeclaration = std::dynamic_pointer_cast<ASTColumnDeclaration>(ast);
    if ( astColumnDeclaration )
    {
        boost::to_upper(astColumnDeclaration.name);
    }

    //type not ASTColumnDeclaration, need to continue
    return;
}

}

