#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Query/ProtosHelper/ExchangeMode.h>


namespace DB
{

    class LocalExchangeStepExt : public ITransformingStep
    {
    public:
        //todo: zhangwanyun1, need optimizer: need Partitioning from Optimizer/Property/Property.h
        // explicit LocalExchangeStep(const DataStream & input_stream_, const ExchangeMode & mode_, Partitioning schema_);
        explicit LocalExchangeStepExt(const DataStream & input_stream_, const RExchangeMode::Enum & mode_);

        String getName() const override
        {
            return "LocalExchangeStepExt";
        }

        void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

        const RExchangeMode::Enum & getExchangeMode() const { return exchange_type; }

        //todo: zhangwanyun1, need optimizer: need Partitioning from Optimizer/Property/Property.h
        // const Partitioning & getSchema() const { return schema; }

        Block getHeader() const
        {
            return getOutputStream().header;
        }

        std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const;
        void updateOutputStream() override;

    private:
        RExchangeMode::Enum exchange_type = RExchangeMode::UNKNOWN;
        //todo: zhangwanyun1, need optimizer: need Partitioning from Optimizer/Property/Property.h
        // Partitioning schema;
    };


}
