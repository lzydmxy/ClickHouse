#pragma once

#include <Query/Analyzer/Analysis.h>
#include <Query/Processors/QueryPlan/QueryPlanExt.h>

namespace DB
{

class QueryPlanner
{
public:
    //todo: need impl
    QueryPlanExtPtr plan(ASTPtr & query, Analysis & analysis, ContextMutablePtr context) { return nullptr; } 
};

}
