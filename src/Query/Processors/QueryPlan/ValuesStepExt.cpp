#include <Query/Processors/QueryPlan/ValuesStepExt.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

namespace DB
{
ValuesStepExt::ValuesStepExt(Block header, Fields fields_, size_t rows_) : ISourceStep(DataStream{.header = header}), fields(fields_), rows(rows_)
{
}

void ValuesStepExt::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    Block block;

    for (size_t index = 0; index < fields.size(); ++index)
    {
        auto col = output_stream->header.getByPosition(index).type->createColumn();
        for (size_t i = 0; i < rows; i++)
        {
            col->insert(fields[index]);
        }
        block.insert({std::move(col), output_stream->header.getByPosition(index).type, output_stream->header.getByPosition(index).name});
    }

    pipeline.init(Pipe(std::make_shared<SourceFromSingleChunk>(getOutputStream().header, Chunk(block.getColumns(), block.rows()))));
    for (const auto & processor : pipeline.getProcessors())
        processors.emplace_back(processor);
}

std::shared_ptr<IQueryPlanStep> ValuesStepExt::copy(ContextPtr) const
{
    return std::make_shared<ValuesStepExt>(output_stream->header, fields);
}

}
