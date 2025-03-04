#pragma once

#include <Processors/QueryPlan/UnionStep.h>

namespace DB
{

using OutputToInputs = std::unordered_map<String, std::vector<String>>;

class UnionStepExt : public UnionStep
{
public:
    /// max_threads is used to limit the number of threads for result pipeline.
    UnionStepExt(DataStreams input_streams_, DataStream output_stream_, OutputToInputs output_to_inputs_, size_t max_threads_, bool local_);

    explicit UnionStepExt(DataStreams input_streams_, DataStream output_stream_ = {}, OutputToInputs output_to_inputs_ = {})
        : UnionStepExt(std::move(input_streams_), std::move(output_stream_), std::move(output_to_inputs_), 0, false)
    {
    }

    const OutputToInputs & getOutToInputs() const;
    NameToNameMap getOutToInput(size_t source_idx) const;

    String getName() const override { return "UnionStepExt"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    bool isLocal() const { return local; }

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const;

private:
    bool local;
    OutputToInputs output_to_inputs;
};

}
