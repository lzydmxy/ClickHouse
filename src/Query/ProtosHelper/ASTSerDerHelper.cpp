#include "ASTSerDerHelper.h"
#include <Core/Types.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Parsers/IAST_fwd.h>
#include <Query/ProtosHelper/QueryProto.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

// #include <Parsers/ASTAlterQuery.h>
// #include <Parsers/ASTAsterisk.h>
// #include <Parsers/ASTCheckQuery.h>
// #include <Parsers/ASTColumnDeclaration.h>
// #include <Parsers/ASTColumnsMatcher.h>
// #include <Parsers/ASTColumnsTransformers.h>
// #include <Parsers/ASTConstraintDeclaration.h>
// #include <Parsers/ASTCreateQuery.h>
// #include <Parsers/ASTForeignKeyDeclaration.h>
// #include <Parsers/ASTDeleteQuery.h>
// #include <Parsers/ASTDictionary.h>
// #include <Parsers/ASTDictionaryAttributeDeclaration.h>

// #include <Parsers/Access/ASTCreateQuotaQuery.h>
// #include <Parsers/Access/ASTCreateRoleQuery.h>
// #include <Parsers/Access/ASTCreateRowPolicyQuery.h>
// #include <Parsers/Access/ASTCreateSettingsProfileQuery.h>
// #include <Parsers/Access/ASTCreateUserQuery.h>


//#include <Parsers/ASTAssignment.h>
//#include <Parsers/ASTAutoStatsQuery.h>
//#include <Parsers/ASTBitEngineConstraintDeclaration.h>
//#include <Parsers/ASTClusterByElement.h>
//#include <Parsers/ASTCreateQueryAnalyticalMySQL.h>
//#include <Parsers/ASTDataType.h>
//#include <Parsers/ASTAdviseQuery.h>
//#include <Parsers/ASTAlterDiskCacheQuery.h>
//#include <Parsers/ASTUniqueNotEnforcedDeclaration.h>

// #include <Parsers/ASTDropAccessEntityQuery.h>
// #include <Parsers/ASTDropQuery.h>
// #include <Parsers/ASTDumpQuery.h>
// #include <Parsers/ASTExplainQuery.h>
// #include <Parsers/ASTExpressionList.h>
// #include <Parsers/ASTExternalDDLQuery.h>
// #include <Parsers/ASTFieldReference.h>
// #include <Parsers/ASTFunction.h>
// #include <Parsers/ASTFunctionWithKeyValueArguments.h>
// #include <Parsers/ASTGrantQuery.h>
// #include <Parsers/ASTIdentifier.h>
// #include <Parsers/ASTIndexDeclaration.h>
// #include <Parsers/ASTInsertQuery.h>
// #include <Parsers/ASTKillQueryQuery.h>
// #include <Parsers/ASTLiteral.h>
// #include <Parsers/ASTNameTypePair.h>
// #include <Parsers/ASTOptimizeQuery.h>
// #include <Parsers/ASTOrderByElement.h>
// #include <Parsers/ASTPartToolKit.h>
// #include <Parsers/ASTPartition.h>
// #include <Parsers/ASTPreparedParameter.h>
// #include <Parsers/ASTPreparedStatement.h>
// #include <Parsers/ASTProjectionDeclaration.h>
// #include <Parsers/ASTProjectionSelectQuery.h>
// #include <Parsers/ASTQualifiedAsterisk.h>
// #include <Parsers/ASTQuantifiedComparison.h>
// #include <Parsers/ASTQueryParameter.h>
// #include <Parsers/ASTQueryWithOnCluster.h>
// #include <Parsers/ASTQueryWithOutput.h>
// #include <Parsers/ASTRefreshQuery.h>
// #include <Parsers/ASTRenameQuery.h>
// #include <Parsers/ASTReproduceQuery.h>
// #include <Parsers/ASTRolesOrUsersSet.h>
// #include <Parsers/ASTRowPolicyName.h>
// #include <Parsers/ASTSQLBinding.h>
// #include <Parsers/ASTSampleRatio.h>
// #include <Parsers/ASTSelectIntersectExceptQuery.h>
// #include <Parsers/ASTSelectQuery.h>
// #include <Parsers/ASTSelectWithUnionQuery.h>
// #include <Parsers/ASTSetQuery.h>
// #include <Parsers/ASTSetSensitiveQuery.h>
// #include <Parsers/ASTSetRoleQuery.h>
// #include <Parsers/ASTSettingsProfileElement.h>
// #include <Parsers/ASTShowAccessEntitiesQuery.h>
// #include <Parsers/ASTShowAccessQuery.h>
// #include <Parsers/ASTShowCreateAccessEntityQuery.h>
// #include <Parsers/ASTShowGrantsQuery.h>
// #include <Parsers/ASTShowTablesQuery.h>
// #include <Parsers/ASTStatsQuery.h>
// #include <Parsers/ASTSubquery.h>
// #include <Parsers/ASTSwitchQuery.h>
// #include <Parsers/ASTSystemQuery.h>
// #include <Parsers/ASTTEALimit.h>
// #include <Parsers/ASTTTLElement.h>
// #include <Parsers/ASTTableColumnReference.h>
// #include <Parsers/ASTTablesInSelectQuery.h>
// #include <Parsers/ASTUpdateQuery.h>
// #include <Parsers/ASTUseQuery.h>
// #include <Parsers/ASTUserNameWithHost.h>
// #include <Parsers/ASTWatchQuery.h>
// #include <Parsers/ASTWindowDefinition.h>
// #include <Parsers/ASTWithElement.h>
// #include <Parsers/queryToString.h>



namespace DB
{
// ASTPtr createWithASTType(ASTType type, ReadBuffer & buf)
// {
//     switch (type)
//     {
// #define DISPATCH(TYPE) \
//     case ASTType::TYPE: \
//         return TYPE::deserialize(buf);
//         APPLY_AST_TYPES(DISPATCH)
// #undef DISPATCH
//         default:
//             throw Exception("Create using unsupported type.", ErrorCodes::LOGICAL_ERROR);
//     }
// }

void serializeAST(const IAST & ast, WriteBuffer & buf)
{
    writeBinary(true, buf);
    // TODO: wait AST class
    // writeBinary(UInt8(ast.getType()), buf);
    // ast.serialize(buf);
}

void serializeAST(const ConstASTPtr & ast, WriteBuffer & buf)
{
    if (ast)
    {
        writeBinary(true, buf);
        //TODO:
        // writeBinary(UInt8(ast->getType()), buf);
        // ast->serialize(buf);
    }
    else
        writeBinary(false, buf);
}

ASTPtr deserializeAST(ReadBuffer & buf)
{
    bool has_ast;
    readBinary(has_ast, buf);
    if (has_ast)
    {
        UInt8 read_type;
        readBinary(read_type, buf);
        //TODO:
        // auto type = ASTType(read_type);
        //auto ast = createWithASTType(type, buf);
        ASTPtr ast = nullptr;
        return ast;
    }
    else
        return nullptr;
}

void serializeASTToProto(const IAST & ast, RAST & proto)
{
    WriteBufferFromOwnString buf;
    serializeAST(ast, buf);
    proto.set_blob(std::move(buf.str()));
    proto.set_text(queryToString(ast));
}

void serializeASTToProto(const ConstASTPtr & ast, RAST & proto)
{
    if (ast)
        serializeASTToProto(*ast, proto);
    else
        proto.set_blob("");
}

ASTPtr deserializeASTFromProto(const RAST & proto)
{
    if (proto.blob().size() == 0)
        return nullptr;

    ReadBufferFromString buf(proto.blob());
    auto ast = deserializeAST(buf);
    return ast;
}

void serializeASTs(const ASTs & asts, WriteBuffer & buf)
{
    writeVarUInt(asts.size(), buf);

    for (const auto & ast : asts)
    {
        serializeAST(ast, buf);
    }
}

ASTs deserializeASTs(ReadBuffer & buf)
{
    size_t size = 0;
    readVarUInt(size, buf);
    ASTs asts(size);

    for (size_t i = 0; i < size; ++i)
    {
        asts[i] = deserializeAST(buf);
    }

    return asts;
}

ASTPtr deserializeASTWithChildren(ASTs & children, ReadBuffer & buf)
{
    auto ast = deserializeAST(buf);
    if (ast)
        children.push_back(ast);
    return ast;
}

String queryToString(const ASTPtr & query, bool always_quote_identifiers)
{
    return queryToString(*query, always_quote_identifiers);
}

String queryToString(const IAST & query, bool always_quote_identifiers)
{
    //TODO: wait for AST
    //return serializeAST(query, true, always_quote_identifiers);
    return String("");
}

}
