#include <Query/Optimizer/Rewriter/AddCache.h>


#include <memory>


namespace DB
{
// namespace
// {
//     std::shared_ptr<TableScanNode> findLeftMostTableScan(PlanNodePtr node)
//     {
//         if (auto scan = dynamic_pointer_cast<TableScanNode>(node))
//             return scan;
//         else if (!node->getChildren().empty())
//             return findLeftMostTableScan(node->getChildren().front());
//         throw Exception("Left most leaf is not table scan", ErrorCodes::LOGICAL_ERROR);
//     }
//
// } // anonymous namespace
//
// class AddCacheVisitor : public SimplePlanRewriter<Void>
// {
// public:
//     explicit AddCacheVisitor(ContextMutablePtr context_, CTEInfo & cte_info_,
//                              CacheableChecker::RuntimeFilterBuildsAndProbes query_runtime_filters,
//                              PlanSignatureProvider base_signature_provider_)
//         : SimplePlanRewriter(context_, cte_info_)
//         , runtime_filter_checker(std::move(query_runtime_filters), context_)
//         , base_signature_provider(std::move(base_signature_provider_)) {}
//
// protected:
//     PlanNodePtr visitAggregatingNode(AggregatingNode & node, Void &) override;
//     // if need to mark the corresponding merging agg, visit it here
//     // PlanNodePtr visitMergingAggregatedNode(MergingAggregatedNode & node, Void &) override;
//
// private:
//     CacheableChecker::RuntimeFilterChecker runtime_filter_checker;
//     PlanSignatureProvider base_signature_provider;
// };

// todo: lizhuoyu5, other feat: IntermediateResultCache is not necessary for the optimizer at this stage.
// todo: However, we might implement it in the future. For now, we have added the class definition without implementing its functionality.
bool AddCache::rewrite(QueryPlanExt & plan, ContextMutablePtr context) const
{
    // auto query_runtime_filters = CacheableChecker::RuntimeFilterCollector::collect(plan);
    // auto base_signature_provider = PlanSignatureProvider::from(plan, context);
    // AddCacheVisitor visitor{context, plan.getCTEInfo(), std::move(query_runtime_filters), std::move(base_signature_provider)};
    // Void c{};
    // auto result = VisitorUtil::accept(plan.getPlanNode(), visitor, c);
    // plan.update(result);
    return true;
}

// PlanNodePtr AddCacheVisitor::visitAggregatingNode(AggregatingNode & node, Void & c)
// {
//     // check valid for cache: tree structure, determinism
//     auto node_ptr = node.shared_from_this();
//     auto partial_agg_step = node.getStep();
//
//     if (!CacheableChecker::isValidForCache(node_ptr, context))
//         return visitPlanNode(node, c);
//     auto table_scan_node_ptr = findLeftMostTableScan(node_ptr);
//
//     // check runtime filter
//     auto cacheable_runtime_filters_opt = runtime_filter_checker.check(node_ptr);
//     if (!cacheable_runtime_filters_opt)
//         return visitPlanNode(node, c);
//
//     // compute cache param
//     CacheParamBuilder builder(node_ptr, context, std::move(cacheable_runtime_filters_opt.value()), base_signature_provider);
//     CacheParam cache_param = builder.buildCacheParam();
//
//     // set cache param for table scan and agg
//     table_scan_node_ptr->getStep()->getQueryInfo().cache_info
//         = TableScanCacheInfo::create(cache_param.digest, cache_param.dependent_tables.size());
//
//     if (partial_agg_step->isPartial())
//     {
//         // add the cache step above a partial agg
//         partial_agg_step->setStreamingForCache(true);
//
//         // add cache step
//         auto cache_step
//             = std::make_shared<IntermediateResultCacheStep>(node.getStep()->getOutputStream(), cache_param, partial_agg_step->getParams());
//         // debug info below
//         auto hint
//             = std::to_string(context->getRuleId()) + "_AddCache_NodeId-" + std::to_string(node.getId()) + "_Digest-" + cache_param.digest;
//         GraphvizPrinter::printLogicalPlan(*builder.getNormalPlan(), context, hint);
//         cache_step->setCacheOrder(builder.getCacheOrder());
//         cache_step->setIncludedRuntimeFilters(builder.getIncludedRuntimeFilters());
//         cache_step->setIgnoredRuntimeFilters(builder.getIgnoredRuntimeFilters());
//
//         return PlanNodeBase::createPlanNode(context->nextNodeId(), cache_step, PlanNodes{node_ptr});
//     }
//     else
//     {
//         // split final agg into partial and merrge
//         QueryPlanStepPtr partial_agg = std::make_shared<AggregatingStep>(
//             node.getChildren()[0]->getStep()->getOutputStream(),
//             partial_agg_step->getKeys(),
//             partial_agg_step->getKeysNotHashed(),
//             partial_agg_step->getAggregates(),
//             partial_agg_step->getGroupingSetsParams(),
//             /* final */ false,
//             AggregateStagePolicy::DEFAULT,
//             partial_agg_step->getGroupBySortDescription(),
//             partial_agg_step->getGroupings(),
//             partial_agg_step->needOverflowRow(),
//             false,
//             partial_agg_step->isNoShuffle(),
//             /* streaming_for_cache */ true,
//             partial_agg_step->getHints());
//
//         auto partial_agg_node
//             = PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(partial_agg), node.getChildren(), node.getStatistics());
//
//         // add cache step
//         auto cache_step = std::make_shared<IntermediateResultCacheStep>(
//             partial_agg_node->getStep()->getOutputStream(),
//             cache_param,
//             dynamic_cast<AggregatingStep *>(partial_agg_node->getStep().get())->getParams());
//         // debug info below
//         auto hint
//             = std::to_string(context->getRuleId()) + "_AddCache_NodeId-" + std::to_string(node.getId()) + "_Digest-" + cache_param.digest;
//         GraphvizPrinter::printLogicalPlan(*builder.getNormalPlan(), context, hint);
//         cache_step->setCacheOrder(builder.getCacheOrder());
//         cache_step->setIncludedRuntimeFilters(builder.getIncludedRuntimeFilters());
//         cache_step->setIgnoredRuntimeFilters(builder.getIgnoredRuntimeFilters());
//         auto cache_node = PlanNodeBase::createPlanNode(context->nextNodeId(), cache_step, PlanNodes{partial_agg_node});
//
//         Names keys;
//         keys.insert(keys.end(), partial_agg_step->getKeys().begin(), partial_agg_step->getKeys().end());
//
//         ColumnNumbers keys_positions;
//         auto exchange_header = partial_agg_node->getStep()->getOutputStream().header;
//
//         for (const auto & key : keys)
//             keys_positions.emplace_back(exchange_header.getPositionByName(key));
//
//         Aggregator::Params new_params(
//             exchange_header,
//             keys_positions,
//             partial_agg_step->getAggregates(),
//             partial_agg_step->getParams().overflow_row,
//             context->getSettingsRef().max_threads);
//
//         auto transform_params = std::make_shared<AggregatingTransformParams>(new_params, partial_agg_step->isFinal());
//         std::shared_ptr<MergingAggregatedStep> final_agg = std::make_shared<MergingAggregatedStep>(
//             partial_agg_node->getStep()->getOutputStream(),
//             std::move(keys),
//             partial_agg_step->getGroupingSetsParams(),
//             partial_agg_step->getGroupings(),
//             transform_params,
//             false,
//             context->getSettingsRef().max_threads,
//             context->getSettingsRef().aggregation_memory_efficient_merge_threads);
//
//         auto final_agg_node
//             = PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(final_agg), {cache_node}, node.getStatistics());
//         return final_agg_node;
//     }
// }

}
