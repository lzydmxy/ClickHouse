#pragma once

#include <Interpreters/InterpreterExplainQuery.h>
#include <Query/Parsers/ASTExplainQueryExt.h>

namespace DB
{

class InterpreterExplainQueryExt : public InterpreterExplainQuery
{
public:
    InterpreterExplainQueryExt(const ASTPtr & query_, ContextPtr context_) : InterpreterExplainQuery(query_, context_) { }

    BlockIO execute() override;

    static Block getSampleBlock(ASTExplainQueryExt::ExplainKindExt kind);

private:
    ASTPtr query;

    QueryPipeline executeImpl();
};

}

