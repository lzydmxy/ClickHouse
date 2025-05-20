#include <Query/Processors/QueryPlan/FinalSampleStepExt.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>

namespace DB
{
void FinalSampleStepExt::toProto(Protos::FinalSampleStepExt & proto, bool) const
{
    ProtosSerDerHelper::serializeToProtoBase(*this, *proto.mutable_query_plan_base());
    proto.set_sample_size(sample_size);
    proto.set_max_chunk_size(max_chunk_size);
}

std::shared_ptr<FinalSampleStepExt> FinalSampleStepExt::fromProto(const Protos::FinalSampleStepExt & proto, ContextPtr)
{
    auto [step_description, base_input_stream] = ProtosSerDerHelper::deserializeFromProtoBase(proto.query_plan_base());
    auto sample_size = proto.sample_size();
    auto max_chunk_size = proto.max_chunk_size();
    auto step = std::make_shared<FinalSampleStepExt>(base_input_stream, sample_size, max_chunk_size);
    step->setStepDescription(step_description);
    return step;
}
}
