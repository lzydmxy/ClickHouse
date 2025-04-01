#pragma once

#include <Query/Analyzer/Analysis.h>
#include <Query/Processors/QueryPlan/QueryPlanExt.h>

namespace DB
{

class QueryPlanner
{
public:
    //todo: need impl, now just a fake impl for build
    QueryPlanExtPtr plan(ASTPtr & query, Analysis & analysis, ContextMutablePtr context) { return nullptr; } 
    //todo: need impl 
    //RelationPlan planQuery(ASTPtr query, TranslationMapPtr outer_query_context, Analysis & analysis, ContextMutablePtr context, CTERelationPlans & cte_info);
};

}
