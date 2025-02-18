#pragma once

#include <Core/Settings.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTDictionary.h>
#include <Parsers/ASTDictionaryAttributeDeclaration.h>
#include <Parsers/IAST_fwd.h>
#include <Query/Parsers/ASTColumnDeclarationExt.h>
#include <Query/Parsers/ASTDictionaryExt.h>

#include <boost/algorithm/string/case_conv.hpp>

namespace DB
{

using DB::IAST;
using DB::ASTPtr;
using DB::ASTs;
using ConstASTPtr = std::shared_ptr<const IAST>;
using ConstASTs = std::vector<ConstASTPtr>;


#define APPLY_AST_TYPES(M) \
    M(ASTAsterisk) \
    M(ASTColumnDeclarationExt) \
    M(ASTDictionaryAttributeDeclaration) \
    M(ASTDictionaryLifetime) \
    M(ASTDictionaryLayout) \
    M(ASTDictionaryRange) \
    M(ASTDictionarySettings) \
    M(ASTDictionaryExt)

#define ENUM_AST_TYPE(ITEM) ITEM,
enum class ASTType : UInt8
{
    APPLY_AST_TYPES(ENUM_AST_TYPE) UNDEFINED,
};
#undef ENUM_AST_TYPE

inline String toString(ASTType type)
{
    switch (type)
    {
#define ENUM_AST_TYPE(ITEM) \
    case ASTType::ITEM: \
        return #ITEM;
        APPLY_AST_TYPES(ENUM_AST_TYPE)
#undef ENUM_AST_TYPE
        default:
            return "UNDEFINED";
    }
}

#define CHECK_AND_RETURN_AST_TYPE(type) \
if (auto * casted_ast = ast->as<type>()) \
{ \
    return ASTType::type; \
}

inline ASTType getAstType(const ASTPtr & ast)
{
    APPLY_AST_TYPES(CHECK_AND_RETURN_AST_TYPE)
    return ASTType::UNDEFINED;
}
#undef CHECK_AND_RETURN_AST_TYPE

void astToLowerCase(const ASTPtr & ast);
void astToUpperCase(const ASTPtr & ast);

void setOrReplaceAST(ASTPtr & cur_ast, ASTPtr & old_child, const ASTPtr & new_child);

}

