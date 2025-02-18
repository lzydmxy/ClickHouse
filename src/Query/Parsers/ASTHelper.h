#pragma once

#include <Core/Settings.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTFunctionWithKeyValueArguments.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Functions/JSONPath/ASTs/ASTJSONPath.h>
#include <Functions/JSONPath/ASTs/ASTJSONPathMemberAccess.h>
#include <Functions/JSONPath/ASTs/ASTJSONPathQuery.h>
#include <Functions/JSONPath/ASTs/ASTJSONPathRange.h>
#include <Functions/JSONPath/ASTs/ASTJSONPathRoot.h>
#include <Functions/JSONPath/ASTs/ASTJSONPathStar.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTDictionary.h>
#include <Parsers/ASTDictionaryAttributeDeclaration.h>
#include <Query/Parsers/ASTColumnDeclarationExt.h>
#include <Query/Parsers/ASTDataTypeExt.h>
#include <Query/Parsers/ASTDictionaryExt.h>
#include <Query/Parsers/ASTExpressionListExt.h>
#include <Query/Parsers/ASTFieldReferenceExt.h>
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


#define APPLY_AST_TYPES(M) \
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
    M(ASTAsterisk) \
    M(ASTColumnsRegexpMatcher) \
    M(ASTColumnsListMatcher) \
    M(ASTConstraintDeclaration) \
    M(ASTDataTypeExt) \
    M(ASTColumnDeclarationExt) \
    M(ASTDictionaryAttributeDeclaration) \
    M(ASTDictionaryLifetime) \
    M(ASTDictionaryLayout) \
    M(ASTDictionaryRange) \
    M(ASTDictionarySettings) \
    M(ASTDictionaryExt) \
    M(ASTExpressionListExt) \
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
    M(ASTTTLElement) \
    M(ASTFunctionWithKeyValueArguments) \
    M(ASTIndexDeclaration) \
    M(ASTJSONPath) \
    M(ASTJSONPathMemberAccess) \
    M(ASTJSONPathQuery) \
    M(ASTJSONPathRange) \
    M(ASTJSONPathRoot) \
    M(ASTJSONPathStar) \
    M(ASTNameTypePair) \
    M(ASTOrderByElement) \

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

