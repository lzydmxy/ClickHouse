#include <Query/Processors/QueryPlan/LocalExchangeStepExt.h>

#include <Processors/ResizeProcessor.h>
#include <Processors/Transforms/ScatterByPartitionTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>

namespace DB
{
LocalExchangeStepExt::LocalExchangeStepExt(const DataStream & input_stream_, const RExchangeMode::Enum & mode_, Partitioning schema_)
    : ITransformingStep(input_stream_, input_stream_.header, {}), exchange_type(mode_), schema(std::move(schema_))
{
}

void LocalExchangeStepExt::updateOutputStream()
{
    output_stream->header = input_streams[0].header;
}

void LocalExchangeStepExt::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & build_context)
{
    auto streams = pipeline.getNumStreams();
    auto stream_header = pipeline.getHeader();

    if (streams <= 1)
    {
        /// Same as round-robin shuffle
        pipeline.addTransform(std::make_shared<ResizeProcessor>(
            stream_header, 1, build_context.getBuildQueryPipelineSettingsExt().context->getSettingsRef().max_threads));
        return;
    }

    ColumnNumbers key_columns;

    key_columns.reserve(schema.getColumns().size());
    for (const auto & name : schema.getColumns())
        key_columns.push_back(stream_header.getPositionByName(name));

    pipeline.transform([&](OutputPortRawPtrs ports) {
        Processors processors;
        for (auto * port : ports)
        {
            auto scatter = std::make_shared<ScatterByPartitionTransform>(stream_header, pipeline.getNumStreams(), key_columns);
            connect(*port, scatter->getInputs().front());
            processors.push_back(scatter);
        }
        return processors;
    });

    pipeline.transform([&](OutputPortRawPtrs ports) {
        Processors processors;
        for (size_t i = 0; i < streams; ++i)
        {
            size_t output_it = i;
            auto resize = std::make_shared<ResizeProcessor>(ports[output_it]->getHeader(), streams, 1);
            auto & inputs = resize->getInputs();

            for (auto input_it = inputs.begin(); input_it != inputs.end(); output_it += streams, ++input_it)
                connect(*ports[output_it], *input_it);
            processors.push_back(resize);
        }
        return processors;
    });

}

void LocalExchangeStepExt::toProto(Protos::LocalExchangeStepExt & proto, bool /*for_hash_equals*/) const
{
    ProtosSerDerHelper::serializeToProtoBase(*this, *proto.mutable_query_plan_base());
    proto.set_exchange_type(exchange_type);
    schema.toProto(*proto.mutable_schema());
}

std::shared_ptr<LocalExchangeStepExt> LocalExchangeStepExt::fromProto(const Protos::LocalExchangeStepExt & proto, ContextPtr /*context*/)
{
    auto [step_description, base_input_stream] = ProtosSerDerHelper::deserializeFromProtoBase(proto.query_plan_base());
    auto schema = Partitioning::fromProto(proto.schema());
    auto step = std::make_shared<LocalExchangeStepExt>(base_input_stream, proto.exchange_type(), schema);
    step->setStepDescription(step_description);
    return step;
}

std::shared_ptr<IQueryPlanStep> LocalExchangeStepExt::copy(ContextPtr) const
{
    return std::make_shared<LocalExchangeStepExt>(input_streams[0], exchange_type, schema);
}

}
