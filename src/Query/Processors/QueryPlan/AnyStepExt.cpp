#include <Query/Processors/QueryPlan/AnyStepExt.h>

namespace DB
{

QueryPipelineBuilderPtr AnyStepExt::updatePipeline(QueryPipelineBuilders, const BuildQueryPipelineSettings &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "AnyStep is a fake step");
}

std::shared_ptr<IQueryPlanStep> AnyStepExt::copy(ContextPtr) const
{
    return std::make_unique<AnyStepExt>(output_stream.value(), group_id);
}

}
