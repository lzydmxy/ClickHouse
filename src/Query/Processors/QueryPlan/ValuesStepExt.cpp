#include <Query/Processors/QueryPlan/ValuesStepExt.h>
#include <QueryPipeline/QueryPipeline.h>

/*
#include <DataStreams/OneBlockInputStream.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPlan/PlanSerDerHelper.h>
*/

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

    //TODO: implement pipeline build
    //pipeline.init(Pipe(std::make_shared<SourceFromSingleChunk>(getOutputStream().header, Chunk(block.getColumns(), block.rows()))));
    //for (const auto & processor : pipeline.getProcessors())
    //    processors.emplace_back(processor);
}

}
