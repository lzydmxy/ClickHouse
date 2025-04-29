#include <Processors/LimitTransform.h>
#include <Query/Processors/QueryPlan/LimitStepExt.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>
#include <Query/Processors/QueryPlan/SortingStepExt.h>

namespace DB
{

LimitStepExt::LimitStepExt(
    const DataStream & input_stream_,
    size_t limit_,
    size_t offset_,
    bool always_read_till_end_,
    bool with_ties_,
    SortDescription description_,
    bool partial_)
    : LimitStep(input_stream_, limit_, offset_, always_read_till_end_, with_ties_, description_)
    , partial(partial_)
{
}

void LimitStepExt::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto transform = std::make_shared<LimitTransform>(pipeline.getHeader(), limit, offset, pipeline.getNumStreams(), always_read_till_end, with_ties, description);
    pipeline.addTransform(std::move(transform));
}

std::shared_ptr<IQueryPlanStep> LimitStepExt::copy(ContextPtr) const
{
    return std::make_shared<LimitStepExt>(input_streams[0], limit, offset, always_read_till_end, with_ties, description, partial);
}

void LimitStepExt::toProto(Protos::LimitStepExt & proto, bool for_hash_equals) const
{
    ProtosSerDerHelper::serializeToProtoBase(*this, *proto.mutable_query_plan_base());
    proto.set_limit(0);
    proto.set_offset(0);
    setSizeOrVariableToProto(limit, *proto.mutable_limit_or_var());
    setSizeOrVariableToProto(offset, *proto.mutable_offset_or_var());
    proto.set_always_read_till_end(always_read_till_end);
    proto.set_with_ties(with_ties);
    for (const auto & element : description)
        ProtosSerDerHelper::toProto(element, *proto.add_description());
    proto.set_partial(partial);
}

std::shared_ptr<LimitStepExt> LimitStepExt::fromProto(const Protos::LimitStepExt & proto, ContextPtr)
{
    auto [step_description, base_input_stream] = ProtosSerDerHelper::deserializeFromProtoBase(proto.query_plan_base());
    auto limit = proto.limit();
    auto offset = proto.offset();
    auto limit_or_var = getSizeOrVariableFromProto(proto.limit_or_var());
    auto offset_or_var = getSizeOrVariableFromProto(proto.offset_or_var());
    auto always_read_till_end = proto.always_read_till_end();
    auto with_ties = proto.with_ties();
    SortDescription description;
    for (const auto & proto_element : proto.description())
    {
        SortColumnDescription element;
        std::cout<<proto_element.DebugString()<<std::endl;
        ProtosSerDerHelper::fillFromProto(element, proto_element);
        description.emplace_back(std::move(element));
    }
    auto partial = proto.partial();
    size_t limit_tmp = limit_or_var ? std::get<size_t>(limit_or_var.value()) : limit;
    size_t offset_tmp = offset_or_var ? std::get<size_t>(offset_or_var.value()) : offset;
    auto step = std::make_shared<LimitStepExt>(
        base_input_stream,
        limit_tmp,
        offset_tmp,
        always_read_till_end,
        with_ties,
        description,
        partial);
    step->setStepDescription(step_description);
    return step;
}

}
