#pragma once

#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Common/Logger.h>

namespace Poco
{
class Logger;
}

namespace DB
{
using RuntimeFilterId = UInt32;
class CacheParam;

namespace IntermediateResult
{
struct CacheHolder;
}
using CacheHolderPtr = std::shared_ptr<IntermediateResult::CacheHolder>;

class IntermediateResultCacheStepExt : public IQueryPlanStep
{
public:
    // TODO add CacheParam
    IntermediateResultCacheStepExt(const DataStream & input_stream_, /* CacheParam cache_param_,*/ Aggregator::Params aggregator_params_);

    String getName() const override { return "IntermediateResultCacheExt"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & build_settings) override;

    void updateInputStream(DataStream input_stream_);

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const;

    // CacheParam getCacheParam() const { return cache_param; }
    Aggregator::Params getAggregatorParams() const { return aggregator_params; }

    // debug info below, already included in hash
    void setIncludedRuntimeFilters(std::unordered_set<RuntimeFilterId> ids) { included_runtime_filters = std::move(ids); }
    void setIgnoredRuntimeFilters(std::unordered_set<RuntimeFilterId> ids) { ignored_runtime_filters = std::move(ids); }
    void setCacheOrder(Block header) { cache_order = std::move(header); }
    const std::unordered_set<RuntimeFilterId> & getIncludedRuntimeFilters() const { return included_runtime_filters; }
    const std::unordered_set<RuntimeFilterId> & getIgnoredRuntimeFilters() const { return ignored_runtime_filters; }
    const Block & getCacheOrder() const { return cache_order; }

private:
    QueryPipelineBuilderPtr processCacheTransform(
        QueryPipelineBuilders & pipelines, const BuildQueryPipelineSettings & build_settings, CacheHolderPtr cache_holder);

    // CacheParam cache_param;
    Aggregator::Params aggregator_params;

    // debug info, already included in hash
    std::unordered_set<RuntimeFilterId> ignored_runtime_filters;
    std::unordered_set<RuntimeFilterId> included_runtime_filters;
    Block cache_order;
    LoggerPtr log;
};

}
