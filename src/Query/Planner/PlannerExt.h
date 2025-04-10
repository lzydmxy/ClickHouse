#pragma once

#include <Query/Analyzer/Analysis.h>
#include <Query/Planner/PlannerModels.h>

namespace DB
{

class QueryPlanExt;
using QueryPlanExtPtr = std::unique_ptr<QueryPlanExt>;

struct TranslationMap;
using TranslationMapPtr = std::shared_ptr<TranslationMap>;

struct RelationPlan;

class PlannerExt
{
public:
    /**
     * Entry method of planning phase.
     *
     */
    QueryPlanExtPtr plan(ASTPtr & query, Analysis & analysis, ContextMutablePtr context);

    /**
     * Entry method of planning an cross-scoped ASTSelectWithUnionQuery/ASTSelectQuery.
     *
     */
    RelationPlan planQuery(ASTPtr query, TranslationMapPtr outer_query_context, Analysis & analysis, ContextMutablePtr context, CTERelationPlans & cte_info);
};

}
