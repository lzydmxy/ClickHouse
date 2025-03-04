#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <Storages/SelectQueryInfo.h>
#include <Interpreters/StorageID.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Interpreters/DatabaseCatalog.h>

namespace DB
{

class PlanSegmentSourceStepExt : public ISourceStep
{
public:
    explicit PlanSegmentSourceStepExt(Block header_,
                                StorageID storage_id_,
                                const SelectQueryInfo & query_info_,
                                const Names & column_names_,
                                QueryProcessingStage::Enum processed_stage_,
                                size_t max_block_size_,
                                unsigned num_streams_,
                                ContextPtr context_ = nullptr);

    String getName() const override { return "PlanSegmentSourceStepExt"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;
    QueryPlanStepPtr generateStep();
    StorageID getStorageID() const { return storage_id; }
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const;

private:
    StorageID storage_id;
    SelectQueryInfo query_info;
    Names column_names;
    QueryProcessingStage::Enum processed_stage;
    size_t max_block_size;
    unsigned num_streams;
    ContextPtr context;
};

}
