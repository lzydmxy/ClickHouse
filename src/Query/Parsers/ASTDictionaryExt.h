#pragma once

#include <Parsers/ASTDictionary.h>
#include <Query/Parsers/IAST.h>

namespace DB
{

/// AST contains all parts of external dictionary definition except attributes
class ASTDictionaryExt : public ASTDictionary
{
public:
    String clickhouse_db;
    String clickhouse_tb;
    String clickhouse_query;
    String clickhouse_invalidate_query;

    ASTPtr clone() const override;
};

}
