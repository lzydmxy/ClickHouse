#include <Query/Processors/QueryPlan/ValuesStepExt.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>

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

void ValuesStepExt::toProto(Protos::ValuesStepExt & proto, bool for_hash_equals) const
{
    ProtosSerDerHelper::serializeToProtoBase(*this, *proto.mutable_query_plan_base());
    for (const auto & element : fields)
        ProtosSerDerHelper::toProto(element, *proto.add_fields());
    proto.set_rows(rows);
}

std::shared_ptr<ValuesStepExt> ValuesStepExt::fromProto(const Protos::ValuesStepExt & proto, ContextPtr context)
{
    auto base_output_header = ProtosSerDerHelper::deserializeFromProtoBase(proto.query_plan_base());
    Fields fields;
    for (const auto & proto_element : proto.fields())
    {
        auto field = ProtosSerDerHelper::fillFromProto(proto_element);
        fields.emplace_back(*field);
    }
    auto rows = proto.rows();
    auto step = std::make_shared<ValuesStepExt>(base_output_header, fields, rows);

    return step;
}

}
