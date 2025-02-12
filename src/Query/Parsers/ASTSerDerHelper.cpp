#include <Query/Parsers/ASTSerDerHelper.h>

#include <Query/Parsers/ASTSetQuery.h>

#include <Query/Protos/plan_node.pb.h>
#include <Parsers/queryToString.h>

namespace JDDB
{

ASTPtr createWithASTType(ASTType type, ReadBuffer & buf)
{
    switch (type)
    {
#define DISPATCH(TYPE) \
    case ASTType::TYPE: \
        return TYPE::deserialize(buf);
        APPLY_AST_TYPES(DISPATCH)
#undef DISPATCH
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Create using unsupported type.");
    }
}

// TODO: cast ast to jd ast
// void serializeAST(const IAST & ast, WriteBuffer & buf)
// {
//     writeBinary(true, buf);
//     writeBinary(UInt8(dynamic_cast<IAST_EXT&>(ast).getType()), buf);
//     ast.serialize(buf);
// }

// void serializeAST(const ConstASTPtr & ast, WriteBuffer & buf)
// {
//     if (ast)
//     {
//         writeBinary(true, buf);
//         writeBinary(UInt8(ast->getType()), buf);
//         ast->serialize(buf);
//     }
//     else
//         writeBinary(false, buf);
// }

ASTPtr deserializeAST(ReadBuffer & buf)
{
    bool has_ast;
    readBinary(has_ast, buf);
    if (has_ast)
    {
        UInt8 read_type;
        readBinary(read_type, buf);
        auto type = ASTType(read_type);

        auto ast = createWithASTType(type, buf);
        return ast;
    }
    else
        return nullptr;
}

void serializeASTToProto(const IAST & ast, DB::Protos::AST & proto)
{
    WriteBufferFromOwnString buf;
    serializeAST(ast, buf);
    proto.set_blob(std::move(buf.str()));
    proto.set_text(queryToString(ast));
}

void serializeASTToProto(const ConstASTPtr & ast, DB::Protos::AST & proto)
{
    if (ast)
        serializeASTToProto(*ast, proto);
    else
        proto.set_blob("");
}

ASTPtr deserializeASTFromProto(const DB::Protos::AST & proto)
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

}
