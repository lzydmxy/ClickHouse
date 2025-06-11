#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{


class ParserExplainQueryExt : public IParserBase
{
protected:
    const char * end;
    bool allow_settings_after_format_in_insert;
    bool enable_optimizer;

    const char * getName() const override { return "EXPLAIN"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    explicit ParserExplainQueryExt(const char * end_, bool allow_settings_after_format_in_insert_, bool enable_optimizer_)
        : end(end_), allow_settings_after_format_in_insert(allow_settings_after_format_in_insert_), enable_optimizer(enable_optimizer_)
    {
    }
};

}
