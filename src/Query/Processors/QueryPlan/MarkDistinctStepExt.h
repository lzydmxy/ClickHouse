#pragma once

#include <Query/Core/NameToType.h>
// #include <Optimizer/RuntimeFilterUtils.h>
#include <Query/Processors/QueryPlan/Assignment.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Query/Processors/QueryPlan/QueryPlanStepHelper.h>

namespace DB
{
class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class MarkDistinctStepExt : public ITransformingStep
{
public:
    explicit MarkDistinctStepExt(
        const DataStream & input_stream_,
        String marker_symbol_,
        std::vector<String> distinct_symbols_);

    String getName() const override { return "MarkDistinctStepExt"; }
    // QueryPlanStepType getType() const { return QueryPlanStepType::MarkDistinctStepExt; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    // void toProto(Protos::MarkDistinctStepExt & proto, bool for_hash_equals = false) const;
    // static std::shared_ptr<MarkDistinctStepExt> fromProto(const Protos::MarkDistinctStepExt & proto, ContextPtr);

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const;
    void updateInputStreams(const DataStreams & input_streams_);

    String getMarkerSymbol() const { return marker_symbol;}
    const std::vector<String> & getDistinctSymbols() const {return distinct_symbols;}

private:
    String marker_symbol;
    std::vector<String> distinct_symbols;
};

}
