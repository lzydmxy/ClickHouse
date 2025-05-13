#pragma once
#include <Query/Processors/QueryPlan/SetOperationStepExt.h>

namespace DB
{
class ExceptStepExt : public SetOperationStepExt
{
public:
    ExceptStepExt(
        DataStreams input_streams_,
        DataStream output_stream_,
        std::unordered_map<String, std::vector<String>> output_to_inputs_,
        bool distinct_);

    ExceptStepExt(DataStreams input_streams_, DataStream output_stream_, bool distinct_)
        : ExceptStepExt(input_streams_, output_stream_, {}, distinct_)
    {
    }

    String getName() const override { return "ExceptExt"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & context) override;
    void toProto(Protos::ExceptStepExt & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<ExceptStepExt> fromProto(const Protos::ExceptStepExt & proto, ContextPtr context);

    bool isDistinct() const;
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const;

private:
    bool distinct;
};

}
