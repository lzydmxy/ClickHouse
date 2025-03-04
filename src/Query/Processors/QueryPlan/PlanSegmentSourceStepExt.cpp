#include <Query/Processors/QueryPlan/PlanSegmentSourceStepExt.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/IStorage.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Processors/Sources/NullSource.h>

namespace DB
{

    PlanSegmentSourceStepExt::PlanSegmentSourceStepExt(Block header_,
                                       StorageID storage_id_,
                                       const SelectQueryInfo & query_info_,
                                       const Names & column_names_,
                                       QueryProcessingStage::Enum processed_stage_,
                                       size_t max_block_size_,
                                       unsigned num_streams_,
                                       ContextPtr context_)
    : ISourceStep(DataStream{.header = header_})
    , storage_id(storage_id_)
    , query_info(query_info_)
    , column_names(column_names_)
    , processed_stage(processed_stage_)
    , max_block_size(max_block_size_)
    , num_streams(num_streams_)
    , context(std::move(context_))
{
    StoragePtr storage = DatabaseCatalog::instance().getTable({storage_id.database_name, storage_id.table_name}, context);
    storage_id.uuid = storage->getStorageID().uuid;
}

void PlanSegmentSourceStepExt::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    auto step = generateStep();
    if (auto * source = dynamic_cast<ISourceStep *>(step.get()))
        source->initializePipeline(pipeline, settings);
}

QueryPlanStepPtr PlanSegmentSourceStepExt::generateStep()
{
    StoragePtr storage = DatabaseCatalog::instance().getTable({storage_id.database_name, storage_id.table_name}, context);
    auto storage_snapshot = storage->getStorageSnapshot(storage->getInMemoryMetadataPtr(), context);
    QueryPlan query_plan;
    storage->read(query_plan, column_names, storage_snapshot, query_info, context, processed_stage, max_block_size, num_streams);

    Pipe pipe;
    if (pipe.empty())
    {
        auto header = storage->getInMemoryMetadataPtr()->getSampleBlock();
        Pipe null_pipe(std::make_shared<NullSource>(header));
        auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(std::move(null_pipe));
        read_from_pipe->setStepDescription("Read from NullSource");
        return read_from_pipe;
    }
    else
        return std::make_unique<ReadFromStorageStep>(std::move(pipe), step_description, context, query_info);
}

std::shared_ptr<IQueryPlanStep> PlanSegmentSourceStepExt::copy(ContextPtr) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "PlanSegmentSourceStep can not copy");
}

}
