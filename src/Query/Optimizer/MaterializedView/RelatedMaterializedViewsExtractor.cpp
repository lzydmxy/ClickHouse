#include <algorithm>
#include <Query/Optimizer/MaterializedView/RelatedMaterializedViewsExtractor.h>

namespace DB
{

RelatedMaterializedViews RelatedMaterializedViewsExtractor::extract(QueryPlanExt & plan, ContextMutablePtr context_)
{
    Void c;
    RelatedMaterializedViewsExtractor finder{context_, plan.getCTEInfo()};
    VisitorUtil::accept(plan.getPlanNode(), finder, c);
    std::sort(finder.result.materialized_views.begin(), finder.result.materialized_views.end(), [](const auto & left, const auto & right){
        return left.getFullTableName() < right.getFullTableName();
    });
    std::sort(
        finder.result.local_materialized_views.begin(),
        finder.result.local_materialized_views.end(),
        [](const auto & left, const auto & right) { return left.getFullTableName() < right.getFullTableName(); });
    return std::move(finder.result);
}


Void RelatedMaterializedViewsExtractor::visitTableScanStepExtNode(TableScanStepExtNode & node, Void &)
{
    auto table_scan = node.getStep();

    auto views = DatabaseCatalog::instance().getDependentViews(table_scan->getStorageID());
    // auto dependencies = DatabaseCatalog::instance().getLoadingDependencies(table_scan->getStorageID());
    for (const auto & item : views)
        if (visited_materialized_views.emplace(item).second)
            result.materialized_views.emplace_back(item);
    return Void{};
}
}
