#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Query/Processors/QueryPlan/SetOperationStepExt.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

class IntersectStepExt : public SetOperationStepExt
{
public:
    IntersectStepExt(
        DataStreams input_streams_,
        DataStream output_stream_,
        std::unordered_map<String, std::vector<String>> output_to_inputs_,
        bool distinct_);

    IntersectStepExt(DataStreams input_streams_, DataStream output_stream_, bool distinct_)
        : IntersectStepExt(input_streams_, output_stream_, {}, distinct_)
    {
    }

    String getName() const override { return "IntersectExt"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & context) override;
    void toProto(Protos::IntersectStepExt & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<IntersectStepExt> fromProto(const Protos::IntersectStepExt & proto, ContextPtr context);

    bool isDistinct() const;
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const;

private:
    bool distinct;
};

}
