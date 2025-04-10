#pragma once

#include <Parsers/ParserQuery.h>


namespace DB
{

class ParserQueryExt : private ParserQuery
{
private:
    const char * end;
    bool allow_settings_after_format_in_insert;

    const char * getName() const override { return "Query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    explicit ParserQueryExt(const char * end_, bool allow_settings_after_format_in_insert_ = false)
        : ParserQuery(end_,allow_settings_after_format_in_insert_)
    {}
};

}
