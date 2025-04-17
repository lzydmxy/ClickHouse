#pragma once

#include <Parsers/IAST.h>
#include <IO/ReadBuffer.h>
#include <Query/ProtosHelper/QueryProto.h>

namespace DB
{

namespace Protos
{
    class AST;
}

using ConstASTPtr = std::shared_ptr<const IAST>;

//TODO: Confirm how ASTType is defined in 24.3 
//ASTPtr createByASTType(ASTType type, ReadBuffer & buf);

void serializeAST(const ConstASTPtr & ast, WriteBuffer & buf);

void serializeAST(const IAST & ast, WriteBuffer & buf);

void serializeAST(const ConstASTPtr & ast, WriteBuffer & buf);

ASTPtr deserializeAST(ReadBuffer & buf);

void serializeASTToProto(const ConstASTPtr & ast, RAST & proto);
void serializeASTToProto(const IAST & ast, RAST & proto);
ASTPtr deserializeASTFromProto(const RAST & proto);

void serializeASTs(const ASTs & asts, WriteBuffer & buf);

ASTs deserializeASTs(ReadBuffer & buf);

ASTPtr deserializeASTWithChildren(ASTs & children, ReadBuffer & buf);


String queryToString(const ASTPtr & query, bool always_quote_identifiers = false);
String queryToString(const IAST & query, bool always_quote_identifiers = false);

}
