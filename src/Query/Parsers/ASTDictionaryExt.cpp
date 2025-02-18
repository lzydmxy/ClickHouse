#include <Query/Parsers/ASTDictionaryExt.h>


namespace DB
{

ASTPtr ASTDictionaryExt::clone() const
{
    auto res = std::make_shared<ASTDictionaryExt>();

    auto base = ASTDictionary::clone();
    *static_cast<ASTDictionary*>(res.get()) = *dynamic_cast<ASTDictionary*>(base.get());

    res->clickhouse_db = clickhouse_db;
    res->clickhouse_tb = clickhouse_tb;
    res->clickhouse_invalidate_query = clickhouse_invalidate_query;
    res->clickhouse_query = clickhouse_query;

    return res;
}

}
