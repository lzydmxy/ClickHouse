#pragma once

#include <Parsers/ParserSelectQuery.h>

namespace DB
{


class ParserSelectQueryExt : public ParserSelectQuery
{
    protected:
    const char * getName() const override { return "SELECT query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
