#pragma once

#include <Processors/QueryPlan/UnionStep.h>
#include <Query/Processors/QueryPlan/SetOperationStepExt.h>

namespace DB
{

using OutputToInputs = std::unordered_map<String, std::vector<String>>;

class UnionStepExt : public SetOperationStepExt
{
public:
    /// max_threads is used to limit the number of threads for result pipeline.
    UnionStepExt(DataStreams input_streams_, DataStream output_stream_, OutputToInputs output_to_inputs_, size_t max_threads_, bool local_);

    explicit UnionStepExt(DataStreams input_streams_, DataStream output_stream_ = {}, OutputToInputs output_to_inputs_ = {})
        : UnionStepExt(std::move(input_streams_), std::move(output_stream_), std::move(output_to_inputs_), 0, false)
    {
    }

    String getName() const override { return "UnionStepExt"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    size_t getMaxThreads() const { return max_threads; }
    bool isLocal() const { return local; }

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const;

    void toProto(Protos::UnionStepExt & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<UnionStepExt> fromProto(const Protos::UnionStepExt & proto, ContextPtr context);

private:
    Block header;
    size_t max_threads;
    bool local;
};

}
