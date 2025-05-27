#pragma once

#include <Core/Settings.h>

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
#include <Parsers/ASTPartition.h>
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
#include <Parsers/ASTExpressionList.h>
#include <Query/Parsers/ASTFieldReferenceExt.h>
#include <Parsers/ASTSelectQuery.h>
#include <Query/Parsers/ASTTableColumnReference.h>
#include <Query/Parsers/ASTQuantifiedComparisonExt.h>
#include <Query/Parsers/ASTType.h>
#include <Query/Parsers/ASTStatsQueryExt.h>
#include <Query//Parsers/ASTClusterByElementExt.h>

namespace DB
{

using DB::IAST;
using DB::ASTPtr;
using DB::ASTs;
using ConstASTPtr = std::shared_ptr<const IAST>;
using ConstASTs = absl::InlinedVector<ConstASTPtr, 7>;
using ASTFunctionPtr = std::shared_ptr<ASTFunction>;

struct ShowStatsQueryInfoExt;
using ASTShowStatsQueryExt = ASTStatsQueryBaseExt<ShowStatsQueryInfoExt>;
class ASTCreateStatsQueryExt;
struct DropStatsQueryInfoExt;
using ASTDropStatsQueryExt = ASTStatsQueryBaseExt<DropStatsQueryInfoExt>;


//class ASTAutoStatsQueryExt;
//class ASTShowStatsQueryExt;

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
if (auto * casted_ast = ast.as<type>()) \
{ \
    return ASTType::type; \
}

#define CHECK_AND_RETURN_AST_TYPE_PTR(type) \
if (auto * casted_ast = ast->as<type>()) \
{ \
    return ASTType::type; \
}

inline ASTType getAstType(const IAST & ast)
{
    APPLY_AST_TYPES(CHECK_AND_RETURN_AST_TYPE)
    return ASTType::UNDEFINED;
}

inline ASTType getAstType(const ASTPtr & ast)
{
    APPLY_AST_TYPES(CHECK_AND_RETURN_AST_TYPE_PTR)
    return ASTType::UNDEFINED;
}

inline ASTType getAstType(const ConstASTPtr & ast)
{
    APPLY_AST_TYPES(CHECK_AND_RETURN_AST_TYPE_PTR)
    return ASTType::UNDEFINED;
}
#undef CHECK_AND_RETURN_AST_TYPE

void astToLowerCase(const ASTPtr & ast);
void astToUpperCase(const ASTPtr & ast);

void serializeASTImpl(const ConstASTPtr & ast, WriteBuffer & buf);
void serializeASTImpl(const IAST & ast, WriteBuffer & buf);
ASTPtr deserializeASTImpl(ASTType type, ReadBuffer & buf);


void setOrReplaceAST(ASTPtr & cur_ast, ASTPtr & old_child, const ASTPtr & new_child);
void replaceChildren(ASTPtr & ast, ASTs & children_);

ASTFunctionPtr makeASTFunctionWithVectorArgs(ASTFunctionPtr & ast, const String &name, ASTs &&args);

template <class Predicate>
inline typename DB::ASTs::size_type erase_if(DB::ConstASTs & asts, Predicate pred) /// NOLINT(cert-dcl58-cpp)
{
    auto old_size = asts.size();
    asts.erase(std::remove_if(asts.begin(), asts.end(), pred), asts.end());
    return old_size - asts.size();
}

}

