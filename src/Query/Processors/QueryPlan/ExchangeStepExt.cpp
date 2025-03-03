#include <Query/Processors/QueryPlan/ExchangeStepExt.h>


namespace DB
{
ExchangeStepExt::ExchangeStepExt(DataStreams input_streams_, const RExchangeMode::Enum & mode_, bool keep_order_)
    : exchange_type(mode_), keep_order(keep_order_)
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

std::shared_ptr<IQueryPlanStep> ExchangeStepExt::copy(ContextPtr) const
{
    // TODO: need Partitioning
    return std::make_shared<ExchangeStepExt>(input_streams, exchange_type, keep_order);
}


}
