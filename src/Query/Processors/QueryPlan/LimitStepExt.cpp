#include <Processors/LimitTransform.h>
#include <Query/Processors/QueryPlan/LimitStepExt.h>

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

/*
void LimitStepExt::prepare(const PreparedStatementContext & prepared_context)
{
    prepared_context.prepare(limit);
    prepared_context.prepare(offset);
}
*/

}
