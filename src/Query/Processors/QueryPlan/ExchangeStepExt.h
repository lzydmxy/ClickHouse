#pragma once

#include <Query/ProtosHelper/ExchangeMode.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Query/Optimizer/Property/Property.h>

namespace DB
{

class WriteBuffer;
class ReadBuffer;

class ExchangeStepExt : public IQueryPlanStep
{
public:
    explicit ExchangeStepExt(DataStreams input_streams_, const RExchangeMode::Enum & mode_, Partitioning schema_, bool keep_order_ = false);

    String getName() const override { return "ExchangeExt"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & context) override;

    void toProto(Protos::ExchangeStepExt & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<ExchangeStepExt> fromProto(const Protos::ExchangeStepExt & proto, ContextPtr context);

    const RExchangeMode::Enum & getExchangeMode() const { return exchange_type; }
    const Partitioning & getSchema() const { return schema; }

    bool needKeepOrder() const { return keep_order; }
    void setKeepOrder(bool keep_order_) { keep_order = keep_order_; }
    const std::unordered_map<String, std::vector<String>> & getOutToInputs() const { return output_to_inputs; }

    Block getHeader() const { return getOutputStream().header; }
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const;
    void updateOutputStream() override;
    bool isScalable() const { return scalable; }
    void setScalable(bool scalable_) { scalable = scalable_; }
    bool canUpdateInputStream() const override { return true; }

private:
    RExchangeMode::Enum exchange_type = RExchangeMode::UNKNOWN;
    Partitioning schema;
    bool keep_order = false;
    std::unordered_map<String, std::vector<String>> output_to_inputs;
    bool scalable = true;
};


}
