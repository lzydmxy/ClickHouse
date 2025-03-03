#include <Query/Processors/QueryPlan/MultiJoinStepExt.h>

namespace DB
{

QueryPipelineBuilderPtr MultiJoinStepExt::updatePipeline(QueryPipelineBuilders, const BuildQueryPipelineSettings &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "UNREACHABLE MultiJoinStep::updatePipeline()");
}

std::shared_ptr<IQueryPlanStep> MultiJoinStepExt::copy(ContextPtr context) const
{
    return std::make_shared<MultiJoinStepExt>(output_stream.value(), graph);
}

}
