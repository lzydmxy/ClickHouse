#include <Query/Processors/QueryPlan/FilterStepExt.h>
#include <IO/Operators.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Query/Common/PredicateUtils.h>
#include <Query/Executor/RuntimeFilter/RuntimeFilterUtils.h>
#include <Query/Processors/QueryPlan/QueryPlanStepHelper.h>
#include <Query/Processors/Transforms/FilterTransformExt.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>
#include <Common/logger_useful.h>


namespace DB
{

FilterStepExt::FilterStepExt(const DataStream & input_stream_, const ConstASTPtr & filter_, bool remove_filter_column_)
    : FilterStep(input_stream_, nullptr, filter_->getColumnName(), remove_filter_column_)
    , filter(filter_)
{
}

std::shared_ptr<IQueryPlanStep> FilterStepExt::copy(ContextPtr) const
{
    return std::make_shared<FilterStepExt>(input_streams[0], filter->clone(), remove_filter_column);
}

void FilterStepExt::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    ConstASTPtr rewrite_filter = filter;
    if (!actions_dag)
    {
        rewrite_filter = rewriteRuntimeFilter(filter, pipeline, settings);
        actions_dag = QueryPlanStepHelper::createFilterExpressionActions(settings.context, rewrite_filter->clone(), input_streams[0].header);
        filter_column_name = rewrite_filter->getColumnName();
    }

    bool contains_runtime_filter = RuntimeFilterUtils::containsRuntimeFilters(rewrite_filter);
    auto expression = std::make_shared<ExpressionActions>(actions_dag, settings.getActionsSettings());

    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type) {
        bool on_totals = stream_type == QueryPipelineBuilder::StreamType::Totals;
        return std::make_shared<FilterTransformExt>(
            header, expression, filter_column_name, remove_filter_column, on_totals, nullptr, contains_runtime_filter);
    });

    if (!blocksHaveEqualStructure(pipeline.getHeader(), output_stream->header))
    {
        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
                pipeline.getHeader().getColumnsWithTypeAndName(),
                output_stream->header.getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name);
        auto convert_actions = std::make_shared<ExpressionActions>(convert_actions_dag, settings.getActionsSettings());

        pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, convert_actions);
        });
    }
}

ConstASTPtr FilterStepExt::rewriteRuntimeFilter(const ConstASTPtr & filter, QueryPipelineBuilder &, const BuildQueryPipelineSettings & build_context)
{
    auto filters = RuntimeFilterUtils::extractRuntimeFilters(filter);
    if (filters.first.empty())
        return filter;

    bool only_bf = build_context.context->getSettingsRef().enable_rewrite_bf_into_prewhere;

    ASTs predicates = std::move(filters.second);

    if (build_context.context->getSettingsRef().enable_two_stages_prewhere)
    {
        //skip all runtime_filters in FilterStepExt, since all runtime_filters has been moved into TableScanStep.
    }
    else
    {
        for (auto & runtime_filter : filters.first)
        {
            auto description = RuntimeFilterUtils::extractDescription(runtime_filter).value();
            auto runtime_filters
                = RuntimeFilterUtils::createRuntimeFilterForFilter(description, build_context.context->getInitialQueryId(), only_bf);
            predicates.insert(predicates.end(), runtime_filters.begin(), runtime_filters.end());
        }
    }

    return PredicateUtils::combineConjuncts(predicates);
}

std::pair<ConstASTPtr, ConstASTPtr> FilterStepExt::splitLargeInValueList(const ConstASTPtr & filter, UInt64 limit)
{
    absl::InlinedVector<ConstASTPtr,PREDICATE_VECTOR_SIZE> removed_large_in_value_list;
    absl::InlinedVector<ConstASTPtr,PREDICATE_VECTOR_SIZE> large_in_value_list;
    for (auto & predicate : PredicateUtils::extractConjuncts(filter))
    {
        LOG_DEBUG(getLogger("FilterStepExt"), " predicate : {}", predicate->formatForErrorMessage());

        if (predicate->as<ASTFunction>() &&
            (predicate->as<const ASTFunction &>().name == "in" ||
             predicate->as<const ASTFunction &>().name == "globalIn" ||
             predicate->as<const ASTFunction &>().name == "notIn" ||
             predicate->as<const ASTFunction &>().name == "globalNotIn"))
        {
            const auto & function = predicate->as<const ASTFunction &>();
            if (function.arguments->children[1]->as<ASTFunction>())
            {
                ASTFunction & tuple = function.arguments->children[1]->as<ASTFunction &>();
                size_t size = tuple.arguments->children.size();
                if (size > limit)
                {
                    large_in_value_list.emplace_back(predicate);
                    continue;
                }
            }
        }
        removed_large_in_value_list.emplace_back(predicate);
    }

    return std::make_pair(
        PredicateUtils::combineConjuncts(removed_large_in_value_list), PredicateUtils::combineConjuncts(large_in_value_list));
}

std::vector<ConstASTPtr> FilterStepExt::removeLargeInValueList(const std::vector<ConstASTPtr> & filters, UInt64 limit)
{
    std::vector<ConstASTPtr> removed_large_in_value_list;
    for (const auto & predicate : filters)
    {
        if (predicate->as<ASTFunction>() &&
           (predicate->as<const ASTFunction &>().name == "in" ||
            predicate->as<const ASTFunction &>().name == "globalIn" ||
            predicate->as<const ASTFunction &>().name == "notIn" ||
            predicate->as<const ASTFunction &>().name == "globalNotIn")
        )
        {
            const auto & function = predicate->as<const ASTFunction &>();
            if (function.arguments->children[1]->as<ASTFunction>())
            {
                ASTFunction & tuple = function.arguments->children[1]->as<ASTFunction &>();
                size_t size = tuple.arguments->children.size();
                if (size > limit)
                {
                    continue;
                }
            }
        }
        removed_large_in_value_list.emplace_back(predicate);
    }
    return removed_large_in_value_list;
}

}
