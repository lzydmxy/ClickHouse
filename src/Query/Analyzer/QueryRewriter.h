#pragma once

#include <Interpreters/Context.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class QueryRewriter
{
public:
    int graphviz_index;
    ASTPtr rewrite(ASTPtr query, ContextMutablePtr context, bool enable_materialized_view = true);
};

}
