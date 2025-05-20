#pragma once

#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTQueryWithOutput.h>

namespace DB
{

/// Parse queries supporting [INTO OUTFILE 'file_name'] [FORMAT format_name] [SETTINGS key1 = value1, key2 = value2, ...] suffix.
class ParserQueryWithOutputExt : public ParserQueryWithOutput
{
protected:
    const char * getName() const override { return "Query with output"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    explicit ParserQueryWithOutputExt(const char * end_, bool allow_settings_after_format_in_insert_ = false)
        : ParserQueryWithOutput(end_,allow_settings_after_format_in_insert_)
    {}
};

}
