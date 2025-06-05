#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Query/ProtosHelper/ExchangeMode.h>
#include <Query/Optimizer/Property/Property.h>


namespace DB
{

    class LocalExchangeStepExt : public ITransformingStep
    {
    public:
        explicit LocalExchangeStepExt(const DataStream & input_stream_, const RExchangeMode::Enum & mode_, Partitioning schema_);

        String getName() const override
        {
            return "LocalExchangeExt";
        }

        void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

        const RExchangeMode::Enum & getExchangeMode() const { return exchange_type; }

        const Partitioning & getSchema() const { return schema; }

        Block getHeader() const
        {
            return getOutputStream().header;
        }

        std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const;
        void updateOutputStream() override;

        void toProto(Protos::LocalExchangeStepExt & proto, bool for_hash_equals = false) const;
        static std::shared_ptr<LocalExchangeStepExt> fromProto(const Protos::LocalExchangeStepExt & proto, ContextPtr context);

    private:
        RExchangeMode::Enum exchange_type = RExchangeMode::UNKNOWN;
        Partitioning schema;
    };


}
