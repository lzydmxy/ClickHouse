#include <Query/Processors/QueryPlan/DistinctStepExt.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/Transforms/DistinctTransform.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>

namespace DB
{

DistinctStepExt::DistinctStepExt(
    const DataStream & input_stream_,
    const SizeLimits & set_size_limits_,
    UInt64 limit_hint_,
    const Names & columns_,
    bool pre_distinct_,
    bool optimize_distinct_in_order_,
    bool can_to_agg_)
    : DistinctStep(input_stream_, set_size_limits_, limit_hint_, columns_, pre_distinct_, optimize_distinct_in_order_)
    , can_to_agg(can_to_agg_)
{
    // diff: byconity has distinct_columns
    /*
    if (!output_stream->distinct_columns.empty() && (!pre_distinct || input_stream_.has_single_port))
    {
        for (const auto & name : columns)
            output_stream->distinct_columns.insert(name);
    }
    */
}

void DistinctStepExt::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    // diff: byconity has distinct_columns
    //if (checkColumnsAlreadyDistinct(columns, input_streams.front().distinct_columns))
    //    return;

    if (!pre_distinct)
        pipeline.resize(1);

    pipeline.addSimpleTransform([&](const Block & header, Pipe::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type != Pipe::StreamType::Main)
            return nullptr;

        return std::make_shared<DistinctTransform>(header, set_size_limits, limit_hint, columns);
    });
}

std::shared_ptr<IQueryPlanStep> DistinctStepExt::copy(ContextPtr) const
{
    return std::make_shared<DistinctStepExt>(input_streams[0], set_size_limits, limit_hint, columns, pre_distinct, optimize_distinct_in_order, can_to_agg);
}

void DistinctStepExt::toProto(Protos::DistinctStepExt & proto, bool for_hash_equals) const
{
    ProtosSerDerHelper::serializeToProtoBase(*this, *proto.mutable_query_plan_base());
    ProtosSerDerHelper::toProto(set_size_limits, *proto.mutable_set_size_limits());
    proto.set_limit_hint(limit_hint);
    for (const auto & element : columns)
        proto.add_columns(element);
    proto.set_pre_distinct(pre_distinct);
}

std::shared_ptr<DistinctStepExt> DistinctStepExt::fromProto(const Protos::DistinctStepExt & proto, ContextPtr context)
{
    auto [step_description, base_input_stream] = ProtosSerDerHelper::deserializeFromProtoBase(proto.query_plan_base());
    SizeLimits set_size_limits;
    ProtosSerDerHelper::fillFromProto(set_size_limits, proto.set_size_limits());
    auto limit_hint = proto.limit_hint();
    std::vector<String> columns;
    for (const auto & element : proto.columns())
        columns.emplace_back(element);
    auto pre_distinct = proto.pre_distinct();
    auto step = std::make_shared<DistinctStepExt>(base_input_stream, set_size_limits, limit_hint, columns, pre_distinct, false, true);
    step->setStepDescription(step_description);
    return step;
}

}
