#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Query/ProtosWrapper/ExchangeMode.h>


namespace DB
{

    class LocalExchangeStepExt : public ITransformingStep
    {
    public:
        // TODO: need Partitioning from Optimizer/Property/Property.h
        // explicit LocalExchangeStep(const DataStream & input_stream_, const ExchangeMode & mode_, Partitioning schema_);
        explicit LocalExchangeStepExt(const DataStream & input_stream_, const RExchangeMode::Enum & mode_);

        String getName() const override
        {
            return "LocalExchange";
        }

        void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

        const RExchangeMode::Enum & getExchangeMode() const { return exchange_type; }

        // // TODO: need Partitioning
        // const Partitioning & getSchema() const { return schema; }

        Block getHeader() const
        {
            return getOutputStream().header;
        }

        void updateOutputStream() override;

    private:
        RExchangeMode::Enum exchange_type = RExchangeMode::UNKNOWN;
        // TODO: need Partitioning
        // Partitioning schema;
    };


}
