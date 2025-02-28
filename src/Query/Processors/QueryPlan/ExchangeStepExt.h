#pragma once

#include <Query/ProtosWrapper/ExchangeMode.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

class WriteBuffer;
class ReadBuffer;

class ExchangeStepExt : public IQueryPlanStep
{
public:
    // TODO: need Partitioning from Optimizer/Property/Property.h
    // explicit ExchangeStepExt(DataStreams input_streams_, const RExchangeMode & mode_,  Partitioning schema_, bool keep_order_ = false);
    explicit ExchangeStepExt(DataStreams input_streams_, const RExchangeMode::Enum & mode_, bool keep_order_ = false);

    String getName() const override { return "Exchange"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & context) override;

    const RExchangeMode::Enum & getExchangeMode() const { return exchange_type; }
    // TODO: need Partitioning
    // const Partitioning & getSchema() const { return schema; }

    bool needKeepOrder() const { return keep_order; }
    void setKeepOrder(bool keep_order_) { keep_order = keep_order_; }
    const std::unordered_map<String, std::vector<String>> & getOutToInputs() const { return output_to_inputs; }

    Block getHeader() const { return getOutputStream().header; }
    void updateOutputStream() override;
    bool isScalable() const { return scalable; }
    void setScalable(bool scalable_) { scalable = scalable_; }

private:
    // friend class QueryPlanStepHelper;
    RExchangeMode::Enum exchange_type = RExchangeMode::UNKNOWN;
    // TODO: need Partitioning
    // Partitioning schema;
    bool keep_order = false;
    std::unordered_map<String, std::vector<String>> output_to_inputs;
    bool scalable = true;
};


}
