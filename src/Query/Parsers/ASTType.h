#pragma once

#include <Core/Settings.h>

/*
#include <Functions/JSONPath/ASTs/ASTJSONPath.h>
#include <Functions/JSONPath/ASTs/ASTJSONPathMemberAccess.h>
#include <Functions/JSONPath/ASTs/ASTJSONPathQuery.h>
#include <Functions/JSONPath/ASTs/ASTJSONPathRange.h>
#include <Functions/JSONPath/ASTs/ASTJSONPathRoot.h>
#include <Functions/JSONPath/ASTs/ASTJSONPathStar.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Parsers/ASTDictionary.h>
#include <Parsers/ASTDictionaryAttributeDeclaration.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTFunctionWithKeyValueArguments.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Parsers/ASTProjectionSelectQuery.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/ASTSampleRatio.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTTLElement.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/Access/ASTRowPolicyName.h>
#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <Parsers/IAST_fwd.h>

#include <Query/Parsers/ASTAutoStatsQueryExt.h>
#include <Query/Parsers/ASTDataTypeExt.h>
#include <Query/Parsers/ASTDictionaryExt.h>
#include <Query/Parsers/ASTExplainQueryExt.h>
#include <Query/Parsers/ASTExpressionListExt.h>
#include <Query/Parsers/ASTFieldReferenceExt.h>
#include <Query/Parsers/ASTSelectQueryExt.h>
#include <Query/Parsers/ASTTableColumnReference.h>
#include <Query/Parsers/ASTQuantifiedComparisonExt.h>
#include <Query/Parsers/ASTStatsQueryExt.h>
*/

namespace DB
{

template <typename StatsQueryInfo> class ASTStatsQueryBaseExt;

struct ShowStatsQueryInfoExt;
using ASTShowStatsQueryExt = ASTStatsQueryBaseExt<ShowStatsQueryInfoExt>;
struct DropStatsQueryInfoExt;
using ASTDropStatsQueryExt = ASTStatsQueryBaseExt<DropStatsQueryInfoExt>;

/*
using DB::IAST;
using DB::ASTPtr;
using DB::ASTs;
using ConstASTPtr = std::shared_ptr<const IAST>;
using ConstASTs = std::vector<ConstASTPtr>;
using ASTFunctionPtr = std::shared_ptr<ASTFunction>;
*/

#define APPLY_AST_TYPES(M) \
    M(ASTArrayJoin) \
    M(ASTAsterisk) \
    M(ASTAutoStatsQueryExt) \
    M(ASTShowStatsQueryExt) \
    M(ASTDropStatsQueryExt) \
    M(ASTCreateStatsQueryExt) \
    M(ASTColumnsApplyTransformer) \
    M(ASTColumnsExceptTransformer) \
    M(ASTColumnsListMatcher) \
    M(ASTColumnsRegexpMatcher) \
    M(ASTColumnsReplaceTransformer) \
    M(ASTConstraintDeclaration) \
    M(ASTDataTypeExt) \
    M(ASTDictionaryAttributeDeclaration) \
    M(ASTDictionaryExt) \
    M(ASTDictionaryLayout) \
    M(ASTDictionaryLifetime) \
    M(ASTDictionaryRange) \
    M(ASTDictionarySettings) \
    M(ASTExplainQueryExt) \
    M(ASTExpressionList) \
    M(ASTFieldReferenceExt) \
    M(ASTFunction) \
    M(ASTFunctionWithKeyValueArguments) \
    M(ASTIdentifier) \
    M(ASTIndexDeclaration) \
    M(ASTJSONPath) \
    M(ASTJSONPathMemberAccess) \
    M(ASTJSONPathQuery) \
    M(ASTJSONPathRange) \
    M(ASTJSONPathRoot) \
    M(ASTJSONPathStar) \
    M(ASTLiteral) \
    M(ASTNameTypePair) \
    M(ASTOrderByElement) \
    M(ASTPair) \
    M(ASTPartition) \
    M(ASTProjectionDeclaration) \
    M(ASTProjectionSelectQuery) \
    M(ASTQualifiedAsterisk) \
    M(ASTQualifiedColumnsRegexpMatcher) \
    M(ASTQualifiedColumnsListMatcher) \
    M(ASTQueryParameter) \
    M(ASTQueryWithOutput) \
    M(ASTRowPolicyName) \
    M(ASTRowPolicyNames) \
    M(ASTSampleRatio) \
    M(ASTSelectIntersectExceptQuery) \
    M(ASTSelectQuery) \
    M(ASTSelectWithUnionQuery) \
    M(ASTSetQuery) \
    M(ASTSettingsProfileElement) \
    M(ASTSettingsProfileElements) \
    M(ASTSubquery) \
    M(ASTTTLElement) \
    M(ASTTableExpression) \
    M(ASTTableIdentifier) \
    M(ASTTableJoin) \
    M(ASTTablesInSelectQuery) \
    M(ASTTablesInSelectQueryElement) \
    M(ASTUseQuery) \
    M(ASTWindowDefinition) \
    M(ASTWindowListElement) \
    M(ASTWithElement) \
    M(ASTTableColumnReference) \
    M(ASTQuantifiedComparisonExt) \
    M(ASTClusterByElementExt)

#define ENUM_AST_TYPE(ITEM) ITEM,
enum class ASTType : UInt8
{
    APPLY_AST_TYPES(ENUM_AST_TYPE) UNDEFINED,
};
#undef ENUM_AST_TYPE

}

