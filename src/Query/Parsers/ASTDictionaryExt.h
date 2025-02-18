#pragma once

#include <Parsers/ASTDictionary.h>
#include <Query/Parsers/IAST.h>

namespace DB
{

class ASTLiteral;


/// AST contains all parts of external dictionary definition except attributes
class ASTDictionaryExt : public ASTDictionary
{
public:
    String clickhouse_db;
    String clickhouse_tb;
    String clickhouse_query;
    String clickhouse_invalidate_query;

    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & format_settings, FormatState & state, FormatStateStacked frame) const override;
};

}
