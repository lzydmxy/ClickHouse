#pragma once

#include <Common/Logger.h>
#include <Query/Optimizer/JoinGraph.h>
#include <Query/Optimizer/MaterializedView/MaterializedViewStructure.h>
#include <Query/Optimizer/MaterializedView/PartitionConsistencyChecker.h>
#include <Query/Optimizer/Rewriter/Rewriter.h>

namespace DB
{
/**
  * MaterializedViewRewriter is based on "Optimizing Queries Using Materialized Views:
  * A Practical, Scalable Solution" by Goldstein and Larson.
  */
class MaterializedViewRewriter : public Rewriter
{
public:
    String name() const override { return "MaterializedViewRewriter"; }

private:
    bool rewrite(QueryPlanExt & plan, ContextMutablePtr context) const override;

    bool rewriteImpl(QueryPlanExt & plan, ContextMutablePtr context) const;

    bool isEnabled(ContextMutablePtr context) const override
    {
        return context->getOptimizerContext()->getSettingsRef().enable_materialized_view_rewrite || context->getOptimizerContext()->getSettingsRef().enable_view_based_query_rewrite;
    }

    LinkedHashMap<MaterializedViewStructurePtr, PartitionCheckResult>
    getRelatedMaterializedViews(QueryPlanExt & plan, ContextMutablePtr context) const;

    LoggerPtr log = getLogger("MaterializedViewRewriter");
};
}
