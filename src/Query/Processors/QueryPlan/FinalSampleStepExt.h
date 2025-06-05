#pragma once

#include <IO/Operators.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <QueryPipeline/SizeLimits.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Common/JSONBuilder.h>
#include <Query/Parsers/ASTHelper.h>
#include <Query/Processors/Transforms/FinalSampleTransformExt.h>
#include <QueryPipeline/QueryPipelineBuilder.h>


namespace DB
{
class FinalSampleTransform;

/// Executes Sample. See FinalSampleTransform.
class FinalSampleStepExt : public ITransformingStep
{
public:
    FinalSampleStepExt(const DataStream & input_stream_, size_t sample_size_, size_t max_chunk_size_)
        : ITransformingStep(input_stream_, input_stream_.header, getTraits()), sample_size(sample_size_), max_chunk_size(max_chunk_size_)
    {
    }

    size_t getSampleSize() const { return sample_size; }
    size_t getMaxChunkSize() const { return max_chunk_size; }

    void setSampleSize(size_t sample_size_) { sample_size = sample_size_; }

    String getName() const override { return "FinalSampleExt"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        auto transform = std::make_shared<FinalSampleTransformExt>(pipeline.getHeader(), sample_size, max_chunk_size, pipeline.getNumStreams());
        pipeline.addTransform(std::move(transform));
    }

    void describeActions(FormatSettings & settings) const override
    {
        String prefix(settings.offset, ' ');
        settings.out << prefix << "sample_size " << sample_size << '\n';
        settings.out << prefix << "max_chunk_size " << max_chunk_size << '\n';
    }

    void describeActions(JSONBuilder::JSONMap & map) const override
    {
        map.add("sample_size", sample_size);
        map.add("max_chunk_size", max_chunk_size);
    }

    void toProto(Protos::FinalSampleStepExt & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<FinalSampleStepExt> fromProto(const Protos::FinalSampleStepExt & proto, ContextPtr context);

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const
    {
        return std::make_shared<FinalSampleStepExt>(input_streams[0], sample_size, max_chunk_size);
    }

    static ITransformingStep::Traits getTraits()
    {
        return ITransformingStep::Traits{
            {
                //todo: liyang453, other feat: need preserves_distinct_columns in ITransformingStep.DataStreamTraits, may be not need in 24.3
                //.preserves_distinct_columns = true,
                .returns_single_stream = false,
                .preserves_number_of_streams = true,
                .preserves_sorting = true,
            },
            {
                .preserves_number_of_rows = false,
            }};
    }

    void setInputStreams(const DataStreams & input_streams_) 
    {
        input_streams = input_streams_;
        output_stream->header = input_streams_[0].header;
    }

private:
    size_t sample_size;
    size_t max_chunk_size;
};

}
