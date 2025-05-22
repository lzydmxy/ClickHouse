#pragma once

#include <Interpreters/InterpreterExplainQuery.h>
#include <Query/Parsers/ASTExplainQueryExt.h>
#include <Common/SettingsChanges.h>

namespace DB
{

class InterpreterExplainQueryExt : public InterpreterExplainQuery
{
public:
    InterpreterExplainQueryExt(const ASTPtr & query_, ContextPtr context_) : InterpreterExplainQuery(query_, context_) { }

    BlockIO execute() override;

    static Block getSampleBlock(ASTExplainQueryExt::ExplainKindExt kind);

    /// To extract SETTINGS clauses from query
    static SettingsChanges extractSettingsFromQuery(const ASTPtr & ast, ContextMutablePtr context);

private:
    ASTPtr query;

    QueryPipeline executeImpl();
};

}

