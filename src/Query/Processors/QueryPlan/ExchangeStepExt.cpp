#include <Query/Processors/QueryPlan/ExchangeStepExt.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>


namespace DB
{
ExchangeStepExt::ExchangeStepExt(DataStreams input_streams_, const RExchangeMode::Enum & mode_, Partitioning schema_, bool keep_order_)
    : exchange_type(mode_), schema(std::move(schema_)), keep_order(keep_order_)
{
    updateInputStreams(input_streams_);
}

void ExchangeStepExt::updateOutputStream()
{
    output_stream = DataStream{.header = input_streams[0].header};
    for (size_t i = 0; i < output_stream->header.columns(); ++i)
    {
        String output_symbol = output_stream->header.getByPosition(i).name;
        std::vector<String> inputs;
        for (auto & input_stream : input_streams)
        {
            String input_symbol = input_stream.header.getByPosition(i).name;
            inputs.emplace_back(input_symbol);
        }
        output_to_inputs[output_symbol] = inputs;
    }
}

QueryPipelineBuilderPtr ExchangeStepExt::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    return std::move(pipelines[0]);
}

std::shared_ptr<ExchangeStepExt> ExchangeStepExt::fromProto(const Protos::ExchangeStepExt & proto, ContextPtr)
{
    DataStreams input_streams;
    for (const auto & proto_element : proto.input_streams())
    {
        DataStream element;
        ProtosSerDerHelper::fillFromProto(element, proto_element);
        input_streams.emplace_back(std::move(element));
    }

    auto schema = Partitioning::fromProto(proto.schema());
    auto keep_order = proto.keep_order();
    auto step = std::make_shared<ExchangeStepExt>(input_streams, proto.exchange_type(), schema, keep_order);

    return step;
}

void ExchangeStepExt::toProto(Protos::ExchangeStepExt & proto, bool) const
{
    for (const auto & element : input_streams)
        ProtosSerDerHelper::toProto(element, *proto.add_input_streams());
    proto.set_exchange_type(exchange_type);
    schema.toProto(*proto.mutable_schema());
    proto.set_keep_order(keep_order);
}

std::shared_ptr<IQueryPlanStep> ExchangeStepExt::copy(ContextPtr) const
{
    //todo: zhangwanyun, other feat: need Partitioning
    return std::make_shared<ExchangeStepExt>(input_streams, exchange_type, schema, keep_order);
}


}
