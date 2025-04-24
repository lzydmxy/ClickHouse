#pragma once

#include <Interpreters/Context.h>
#include <Query/Interpreters/executeSubQuery.h>

namespace DB::QueryStatistics
{
class SubqueryHelper
{
public:
    static ContextMutablePtr createQueryContext(ContextPtr context);
};

}
