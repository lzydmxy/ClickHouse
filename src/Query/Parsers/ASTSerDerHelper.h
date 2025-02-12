#pragma once

#include <Query/Parsers/IAST.h>

namespace DB::Protos
{
class AST;
}

namespace JDDB
{


ASTPtr createByASTType(ASTType type, ReadBuffer & buf);

void serializeAST(const ConstASTPtr & ast, WriteBuffer & buf);

void serializeAST(const IAST & ast, WriteBuffer & buf);

void serializeAST(const ConstASTPtr & ast, WriteBuffer & buf);

ASTPtr deserializeAST(ReadBuffer & buf);

void serializeASTToProto(const ConstASTPtr & ast, DB::Protos::AST & proto);
void serializeASTToProto(const IAST & ast, DB::Protos::AST & proto);
ASTPtr deserializeASTFromProto(const DB::Protos::AST & proto);

void serializeASTs(const ASTs & asts, WriteBuffer & buf);

ASTs deserializeASTs(ReadBuffer & buf);

ASTPtr deserializeASTWithChildren(ASTs & children, ReadBuffer & buf);

}
