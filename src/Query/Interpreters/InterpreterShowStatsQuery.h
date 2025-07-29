#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <Query/Statistics/CollectorSettings.h>


namespace DB
{
class Context;


class InterpreterShowStatsQuery : public IInterpreter, WithContext
{
public:
    InterpreterShowStatsQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) { }

    BlockIO execute() override;


private:
    BlockIO executeTable();
    BlockIO executeAll();
    BlockIO executeColumn();
    BlockIO executeSpecial();

    ASTPtr query_ptr;
    // currently only cache_policy is useful
    QueryStatistics::CollectorSettings collector_settings;
};

}
