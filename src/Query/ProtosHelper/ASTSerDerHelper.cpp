#include "ASTSerDerHelper.h"
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Parsers/IAST_fwd.h>
#include <Query/ProtosHelper/QueryProto.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <Query/Parsers/ASTType.h>
#include <Query/Parsers/ASTHelper.h>



namespace DB
{

// namespace
// {
// ASTPtr createWithASTType(ASTType type, ReadBuffer & buf)
// {
//     switch (type)
//     {
// #define DISPATCH(TYPE) \
//     case ASTType::TYPE: \
//         return deserializeASTImpl(type, buf);
//         APPLY_AST_TYPES(DISPATCH)
// #undef DISPATCH
//         default:
//             throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Create using unsupported type.");
//     }
// }
// }

[[noreturn]]void serializeAST(const IAST & ast, WriteBuffer & buf)
{
    writeBinary(true, buf);
    writeBinary(UInt8(getAstType(ast)), buf);
    serializeASTImpl(ast, buf);
}

void serializeAST(const ConstASTPtr & ast, WriteBuffer & buf)
{
    if (ast)
    {
        serializeAST(*ast, buf);
    }
    else
        writeBinary(false, buf);
}

ASTPtr deserializeAST(ReadBuffer & buf)
{
    bool has_ast = false;
    readBinary(has_ast, buf);
    if (has_ast)
    {
        UInt8 read_type;
        readBinary(read_type, buf);
        auto type = static_cast<ASTType>(read_type);
        return deserializeASTImpl(type, buf);
    }
    else
        return nullptr;
}

[[noreturn]] void serializeASTToProto(const IAST & ast, RAST & proto)
{
    WriteBufferFromOwnString buf;
    serializeAST(ast, buf);
    proto.set_blob(std::move(buf.str()));
    proto.set_text(queryToString(ast));
}

[[noreturn]] void serializeASTToProto(const ConstASTPtr & ast, RAST & proto)
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
