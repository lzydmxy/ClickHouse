#include <Query/Processors/QueryPlan/UnionStepExt.h>

#include <Common/assert_cast.h>
#include <Columns/ColumnConst.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Query/Optimizer/Utils.h>
#include <Query/Processors/QueryPlan/QueryPlanStepHelper.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>
#include <Query/ProtosHelper/PlanSerDerHelper.h>


namespace DB
{

UnionStepExt::UnionStepExt(
DataStreams input_streams_, DataStream output_stream_, OutputToInputs output_to_inputs_, size_t max_threads_, bool local_)
: SetOperationStepExt(input_streams_, output_stream_, output_to_inputs_), max_threads(max_threads_), local(local_)
{
    header = Block();
    for (auto & item : output_stream->header)
        header.insert(ColumnWithTypeAndName(item.type, item.name));

    if (header.columns() > 1 && header.has("_dummy"))
        header.erase("_dummy");
}


QueryPipelineBuilderPtr UnionStepExt::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings)
{
    auto pipeline = std::make_unique<QueryPipelineBuilder>();

    if (pipelines.empty())
    {
        QueryPipelineProcessorsCollector collector(*pipeline, this);
        pipeline->init(Pipe(std::make_shared<NullSource>(output_stream->header)));
        processors = collector.detachProcessors();
        return pipeline;
    }

    size_t index = 0;
    for (auto & cur_pipeline : pipelines)
    {
        ASTPtr expr_list = std::make_shared<ASTExpressionList>();
        NamesWithAliases output_names;
        bool need_rename = false;

        for (const auto & item : output_stream->header)
        {
            auto rename_from = output_to_inputs.at(item.name).at(index);
            output_names.emplace_back(rename_from, item.name);
            ASTPtr identifier = std::make_shared<ASTIdentifier>(rename_from);
            identifier->setAlias(item.name);
            expr_list->children.emplace_back(identifier);
            if (item.name != rename_from)
            {
                need_rename = true;
            }
        }

        if (need_rename)
        {
            auto project_action
                = QueryPlanStepHelper::createExpressionActions(Context::getGlobalContextInstance(), cur_pipeline->getHeader().getNamesAndTypesList(), output_names, expr_list);
            auto expression = std::make_shared<ExpressionActions>(project_action, settings.getActionsSettings());
            cur_pipeline->addSimpleTransform(
                [&](const Block & header_) { return std::make_shared<ExpressionTransform>(header_, expression); });

            if (!blocksHaveEqualStructure(cur_pipeline->getHeader(), getOutputStream().header))
            {
                auto actions_dag = ActionsDAG::makeConvertingActions(
                    cur_pipeline->getHeader().getColumnsWithTypeAndName(),
                    getOutputStream().header.getColumnsWithTypeAndName(),
                    ActionsDAG::MatchColumnsMode::Position);
                auto converting_actions = std::make_shared<ExpressionActions>(std::move(actions_dag));
                cur_pipeline->addSimpleTransform(
                    [&](const Block & cur_header) { return std::make_shared<ExpressionTransform>(cur_header, converting_actions); });
            }
        }

        /// Headers for union must be equal.
        /// But, just in case, convert it to the same header if not.
        if (!isCompatibleHeader(cur_pipeline->getHeader(), getOutputStream().header))
        {
            auto converting_dag = ActionsDAG::makeConvertingActions(
                cur_pipeline->getHeader().getColumnsWithTypeAndName(),
                getOutputStream().header.getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name);

            auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));
            cur_pipeline->addSimpleTransform(
                [&](const Block & cur_header) { return std::make_shared<ExpressionTransform>(cur_header, converting_actions); });
        }
        index++;
    }

    *pipeline = QueryPipelineBuilder::unitePipelines(std::move(pipelines), getMaxThreads());
    return pipeline;
}

std::shared_ptr<IQueryPlanStep> UnionStepExt::copy(ContextPtr) const
{
    return std::make_shared<UnionStepExt>(input_streams, output_stream.value(), output_to_inputs, max_threads, local);
}

std::shared_ptr<UnionStepExt> UnionStepExt::fromProto(const Protos::UnionStepExt & proto, ContextPtr)
{
    auto [base_input_streams, base_output_stream, output_to_inputs] = SetOperationStepExt::deserializeFromProtoBase(proto.query_plan_base());
    auto max_threads = proto.max_threads();
    auto local = proto.local();
    auto step = std::make_shared<UnionStepExt>(base_input_streams, base_output_stream, output_to_inputs, max_threads, local);

    return step;
}

void UnionStepExt::toProto(Protos::UnionStepExt & proto, bool) const
{
    SetOperationStepExt::serializeToProtoBase(*proto.mutable_query_plan_base());
    proto.set_max_threads(max_threads);
    proto.set_local(local);
}

}
