#include <Interpreters/ExpressionActions.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Query/Processors/QueryPlan/QueryPlanStepHelper.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include "Query/ProtosHelper/ProtosSerDerHelper.h"

#pragma clang diagnostic ignored "-Wmissing-noreturn"

namespace DB
{
QueryPlanStepPtr QueryPlanStepHelper::copyQueryPlanStep(const QueryPlanStepPtr & query_plan_step, ContextPtr context)
{
    if (auto step_ptr = std::dynamic_pointer_cast<OffsetStep>(query_plan_step))
        return std::make_shared<OffsetStep>(step_ptr->input_streams[0], step_ptr->offset);
    else if (auto step_ptr = std::dynamic_pointer_cast<ArrayJoinStep>(query_plan_step))
        return std::make_shared<ArrayJoinStep>(step_ptr->input_streams[0], step_ptr->array_join);
    else if (auto step_ptr = std::dynamic_pointer_cast<CubeStep>(query_plan_step))
        return std::make_shared<CubeStep>(step_ptr->input_streams[0], step_ptr->params, step_ptr->final, step_ptr->use_nulls);
    else if (auto step_ptr = std::dynamic_pointer_cast<ExpressionStep>(query_plan_step))
        return std::make_shared<ExpressionStep>(step_ptr->input_streams[0], step_ptr->actions_dag);
    else if (auto step_ptr = std::dynamic_pointer_cast<ExtremesStep>(query_plan_step))
        return std::make_shared<ExtremesStep>(step_ptr->input_streams[0]);
    else if (auto step_ptr = std::dynamic_pointer_cast<LimitByStep>(query_plan_step))
        return std::make_shared<LimitByStep>(step_ptr->input_streams[0], step_ptr->group_length, step_ptr->group_offset, step_ptr->columns);
    else if (auto step_ptr = std::dynamic_pointer_cast<FilledJoinStep>(query_plan_step))
        return std::make_shared<FilledJoinStep>(step_ptr->input_streams[0], step_ptr->join, step_ptr->max_block_size);
    else if (auto step_ptr = std::dynamic_pointer_cast<ReadFromPreparedSource>(query_plan_step))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ReadFromPreparedSource can not copy");
    else if (auto step_ptr = std::dynamic_pointer_cast<ReadFromStorageStep>(query_plan_step))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ReadFromStorageStep can not copy");
    else if (auto step_ptr = std::dynamic_pointer_cast<RollupStep>(query_plan_step))
        return std::make_shared<RollupStep>(step_ptr->input_streams[0], step_ptr->params, step_ptr->final, step_ptr->use_nulls);
    else if (auto step_ptr = std::dynamic_pointer_cast<IntersectOrExceptStep>(query_plan_step))
        return std::make_shared<IntersectOrExceptStep>(step_ptr->input_streams, step_ptr->current_operator, step_ptr->max_threads);
    else if (auto step_ptr = std::dynamic_pointer_cast<CreatingSetStep>(query_plan_step))
    {
        auto set_and_key = std::make_shared<SetAndKey>();
        set_and_key->key = step_ptr->set_and_key->key;
        set_and_key->set = step_ptr->set_and_key->set;

        return std::make_shared<CreatingSetStep>(
            step_ptr->input_streams[0],
            set_and_key,
            step_ptr->external_table,
            step_ptr->network_transfer_limits,
            step_ptr->context);
    }
    else if (auto step_ptr = std::dynamic_pointer_cast<AggregatingStepExt>(query_plan_step))
    {
        return std::make_shared<AggregatingStepExt>(
            step_ptr->input_streams[0],
            step_ptr->keys,
            step_ptr->keys_not_hashed,
            step_ptr->params.aggregates,
            step_ptr->grouping_sets_params,
            step_ptr->final,
            step_ptr->stage_policy,
            step_ptr->group_by_sort_description,
            step_ptr->groupings,
            step_ptr->needOverflowRow(),
            step_ptr->should_produce_results_in_order_of_bucket_number,
            step_ptr->no_shuffle,
            step_ptr->streaming_for_cache);
    }
    else if (auto step_ptr = std::dynamic_pointer_cast<MergingAggregatedStep>(query_plan_step))
    {
        return std::make_shared<MergingAggregatedStep>(
            step_ptr->input_streams[0],
            step_ptr->params,
            step_ptr->final,
            step_ptr->memory_efficient_aggregation,
            step_ptr->max_threads,
            step_ptr->memory_efficient_merge_threads,
            step_ptr->should_produce_results_in_order_of_bucket_number,
            step_ptr->max_block_size,
            step_ptr->memory_bound_merging_max_block_bytes,
            step_ptr->group_by_sort_description,
            step_ptr->memory_bound_merging_of_aggregation_results_enabled);
    }
    else if (auto window_step = std::dynamic_pointer_cast<WindowStep>(query_plan_step))
    {
        return std::make_shared<WindowStep>(
            window_step->input_streams[0],
            window_step->window_description,
            window_step->window_functions,
            window_step->streams_fan_out);
    }
    else if (auto sorting_step = std::dynamic_pointer_cast<SortingStep>(query_plan_step))
    {
        switch (sorting_step->getType())
        {
            case SortingStep::Type::FinishSorting:
                return std::make_shared<SortingStep>(
                    sorting_step->input_streams[0],
                    sorting_step->prefix_description,
                    sorting_step->result_description,
                    sorting_step->sort_settings.max_block_size,
                    sorting_step->limit);
            case SortingStep::Type::Full:
                if (!sorting_step->partition_by_description.empty())
                {
                    return std::make_shared<SortingStep>(
                        sorting_step->input_streams[0],
                        sorting_step->result_description,
                        sorting_step->partition_by_description,
                        sorting_step->limit,
                        sorting_step->sort_settings,
                        sorting_step->optimize_sorting_by_input_stream_properties);
                }
                else
                {
                    return std::make_shared<SortingStep>(
                        sorting_step->input_streams[0],
                        sorting_step->result_description,
                        sorting_step->limit,
                        sorting_step->sort_settings,
                        sorting_step->optimize_sorting_by_input_stream_properties);
                }
            case SortingStep::Type::MergingSorted:
                return std::make_shared<SortingStep>(
                    sorting_step->input_streams[0],
                    sorting_step->result_description,
                    sorting_step->sort_settings.max_block_size,
                    sorting_step->always_read_till_end);
        }
    }
    else if (auto filling_step = std::dynamic_pointer_cast<FillingStep>(query_plan_step))
    {
        return std::make_shared<FillingStep>(
            filling_step->input_streams[0],
            filling_step->sort_description,
            filling_step->fill_description,
            filling_step->interpolate_description,
            filling_step->use_with_fill_by_sorting_prefix);
    }
    else if (auto aggregating_projection_step = std::dynamic_pointer_cast<AggregatingProjectionStep>(query_plan_step))
    {
        return std::make_shared<AggregatingProjectionStep>(
            aggregating_projection_step->input_streams,
            aggregating_projection_step->params,
            aggregating_projection_step->final,
            aggregating_projection_step->merge_threads,
            aggregating_projection_step->temporary_data_merge_threads);
    }
    else if (auto step_ptr = std::dynamic_pointer_cast<CreatingSetsStep>(query_plan_step))
        return std::make_shared<CreatingSetsStep>(step_ptr->getInputStreams());

    // StepExt uses macros to execute copy
#define CHECK_AND_COPY_QUERY_PLAN_STEP_EXT(type) \
if (auto step_ptr = std::dynamic_pointer_cast<type>(query_plan_step)) \
{ \
return step_ptr->copy(context); \
}

    APPLY_ALL_STEP_TYPES_FOR_EXT(CHECK_AND_COPY_QUERY_PLAN_STEP_EXT)
#undef CHECK_AND_COPY_QUERY_PLAN_STEP_EXT

    return nullptr;
}


void QueryPlanStepHelper::toProto(const FillingStep & step, Protos::FillingStep & proto_step, bool)
{
    ProtosSerDerHelper::serializeToProtoBase(step, *proto_step.mutable_query_plan_base());
    for (const auto & element : step.sort_description)
        ProtosSerDerHelper::toProto(element, *proto_step.add_sort_description());
    for (const auto & element : step.fill_description)
        ProtosSerDerHelper::toProto(element, *proto_step.add_fill_description());
    proto_step.set_use_with_fill_by_sorting_prefix(step.use_with_fill_by_sorting_prefix);
}

void QueryPlanStepHelper::toProto(const IntersectOrExceptStep & step, Protos::IntersectOrExceptStep & proto_step, bool)
{
    for (const auto & element : step.input_streams)
        ProtosSerDerHelper::toProto(element, *proto_step.add_input_streams());
    proto_step.set_current_operator(ASTSelectIntersectExceptQueryOperatorConverter::toProto(step.current_operator));
    proto_step.set_max_threads(step.max_threads);
}

void QueryPlanStepHelper::toProto(const ReadNothingStep & step, Protos::ReadNothingStep & proto_step, bool)
{
    serializeHeaderToProto(step.output_stream->header, *proto_step.mutable_query_plan_base()->mutable_output_header());
}

void QueryPlanStepHelper::toProto(const OffsetStep & step, Protos::OffsetStep & proto_step, bool)
{
    ProtosSerDerHelper::serializeToProtoBase(step, *proto_step.mutable_query_plan_base());
    proto_step.set_offset(step.offset);
}

void QueryPlanStepHelper::toProto(const LimitByStep & step, Protos::LimitByStep & proto_step, bool)
{
    ProtosSerDerHelper::serializeToProtoBase(step, *proto_step.mutable_query_plan_base());
    proto_step.set_group_length(step.group_length);
    proto_step.set_group_offset(step.group_offset);

    for (const auto & element : step.columns)
        proto_step.add_columns(element);
}

void QueryPlanStepHelper::toProto(const ExtremesStep & step, Protos::ExtremesStep & proto_step, bool)
{
    ProtosSerDerHelper::serializeToProtoBase(step, *proto_step.mutable_query_plan_base());
}

void QueryPlanStepHelper::toProto(const ArrayJoinStep & step, Protos::ArrayJoinStep & proto_step, bool)
{
    ProtosSerDerHelper::serializeToProtoBase(step, *proto_step.mutable_query_plan_base());
    ProtosSerDerHelper::toProto(*step.arrayJoin(), *proto_step.mutable_array_join());
}

void QueryPlanStepHelper::toProto(const WindowStep & step, Protos::WindowStep & proto_step, bool)
{
    ProtosSerDerHelper::serializeToProtoBase(step, *proto_step.mutable_query_plan_base());
    ProtosSerDerHelper::toProto(step.window_description, *proto_step.mutable_window_description());
    for (const auto & element : step.window_functions)
        ProtosSerDerHelper::toProto(element, *proto_step.add_window_functions());
    proto_step.set_streams_fan_out(step.streams_fan_out);
}

#define TO_PROTO_EXT_IMP(TYPE, VAR_NAME) \
void QueryPlanStepHelper::toProto(const TYPE & step, Protos::TYPE & proto, bool for_hash_equals) \
{ \
step.toProto(proto, for_hash_equals); \
}

APPLY_PROTOBUF_STEP_TYPES_AND_NAMES_FOR_EXT(TO_PROTO_EXT_IMP)
#undef TO_PROTO_EXT_IMP

void QueryPlanStepHelper::toProto(const IQueryPlanStep & query_plan_step, Protos::QueryPlanStep & proto, bool for_hash_equals)
{
    switch (getQueryPlanStepType(query_plan_step))
    {
// 1. StepExt with proto uses macros to execute toProto, see PROTOBUF_STEP_TYPES_AND_NAMES_FOR_EXT
#define CASE_DEF(TYPE, VAR_NAME) \
    case QueryPlanStepType::TYPE: { \
        const auto & step = dynamic_cast<const TYPE &>(query_plan_step); \
        auto *proto_step = proto.mutable_##VAR_NAME(); \
        toProto(step, *proto_step, for_hash_equals); \
        break; \
    }

    APPLY_PROTOBUF_STEP_TYPES_AND_NAMES_FOR_EXT(CASE_DEF)
#undef CASE_DEF

        // 2. Step with proto needs implementing toProto manually, see PROTOBUF_STEP_TYPES_AND_NAMES
        case QueryPlanStepType::FillingStep:
        {
            const auto & step= dynamic_cast<const FillingStep &>(query_plan_step);
            auto *proto_step = proto.mutable_filling_step();
            toProto(step, *proto_step);
            break;
        }
        case QueryPlanStepType::IntersectOrExceptStep:
        {
            const auto & step= dynamic_cast<const IntersectOrExceptStep &>(query_plan_step);
            auto *proto_step = proto.mutable_intersect_or_except_step();
            toProto(step, *proto_step);
            break;
        }
        case QueryPlanStepType::ReadNothingStep:
        {
            const auto & step= dynamic_cast<const ReadNothingStep &>(query_plan_step);
            auto *proto_step = proto.mutable_read_nothing_step();
            toProto(step, *proto_step);
            break;
        }
        case QueryPlanStepType::OffsetStep:
        {
            const auto & step= dynamic_cast<const OffsetStep &>(query_plan_step);
            auto *proto_step = proto.mutable_offset_step();
            toProto(step, *proto_step);
            break;
        }
        case QueryPlanStepType::LimitByStep:
        {
            const auto & step= dynamic_cast<const LimitByStep &>(query_plan_step);
            auto *proto_step = proto.mutable_limit_by_step();
            toProto(step, *proto_step);
            break;
        }
        case QueryPlanStepType::ExtremesStep:
        {
            const auto & step= dynamic_cast<const ExtremesStep &>(query_plan_step);
            auto *proto_step = proto.mutable_extremes_step();
            toProto(step, *proto_step);
            break;
        }
        case QueryPlanStepType::ArrayJoinStep:
        {
            const auto & step = dynamic_cast<const ArrayJoinStep &>(query_plan_step);
            auto *proto_step = proto.mutable_array_join_step();
            toProto(step, *proto_step);
            break;
        }
        case QueryPlanStepType::WindowStep:
        {
            const auto & step = dynamic_cast<const WindowStep &>(query_plan_step);
            auto *proto_step = proto.mutable_window_step();
            toProto(step, *proto_step);
            break;
        }
        default: {
            throw Exception(ErrorCodes::PROTOBUF_BAD_CAST, "not implemented step: {}", static_cast<int>(getQueryPlanStepType(query_plan_step)));
        }
    }
}


QueryPlanStepPtr QueryPlanStepHelper::fromProto(const Protos::FillingStep & proto_step, ContextPtr)
{
    auto [step_description, base_input_stream] = ProtosSerDerHelper::deserializeFromProtoBase(proto_step.query_plan_base());
    SortDescription sort_description;
    for (const auto & proto_element : proto_step.sort_description())
    {
        SortColumnDescription element;
        ProtosSerDerHelper::fillFromProto(element, proto_element);
        sort_description.emplace_back(std::move(element));
    }
    SortDescription fill_description;
    for (const auto & proto_element : proto_step.fill_description())
    {
        SortColumnDescription element;
        ProtosSerDerHelper::fillFromProto(element, proto_element);
        fill_description.emplace_back(std::move(element));
    }
    auto step = std::make_shared<FillingStep>(base_input_stream, sort_description, fill_description, nullptr, proto_step.use_with_fill_by_sorting_prefix());
    step->setStepDescription(step_description);
    return step;
}

QueryPlanStepPtr QueryPlanStepHelper::fromProto(const Protos::IntersectOrExceptStep & proto_step, ContextPtr)
{
    DataStreams input_streams;
    for (const auto & proto_element : proto_step.input_streams())
    {
        DataStream element;
        ProtosSerDerHelper::fillFromProto(element, proto_element);
        input_streams.emplace_back(std::move(element));
    }
    auto current_operator = ASTSelectIntersectExceptQueryOperatorConverter::fromProto(proto_step.current_operator());
    auto max_threads = proto_step.max_threads();
    auto step = std::make_shared<IntersectOrExceptStep>(input_streams, current_operator, max_threads);
    return step;
}

QueryPlanStepPtr QueryPlanStepHelper::fromProto(const Protos::ReadNothingStep & proto_step, ContextPtr)
{
    return std::make_shared<ReadNothingStep>(deserializeHeaderFromProto(proto_step.query_plan_base().output_header()));
}

QueryPlanStepPtr QueryPlanStepHelper::fromProto(const Protos::OffsetStep & proto_step, ContextPtr)
{
    auto [step_description, base_input_stream] = ProtosSerDerHelper::deserializeFromProtoBase(proto_step.query_plan_base());
    auto offset = proto_step.offset();
    auto step = std::make_shared<OffsetStep>(base_input_stream, offset);
    step->setStepDescription(step_description);
    return step;
}

QueryPlanStepPtr QueryPlanStepHelper::fromProto(const Protos::LimitByStep & proto_step, ContextPtr)
{
    auto [step_description, base_input_stream] = ProtosSerDerHelper::deserializeFromProtoBase(proto_step.query_plan_base());
    auto group_length = proto_step.group_length();
    auto group_offset = proto_step.group_offset();
    std::vector<String> columns;
    for (const auto & element : proto_step.columns())
        columns.emplace_back(element);
    auto step = std::make_shared<LimitByStep>(base_input_stream, group_length, group_offset, columns);
    step->setStepDescription(step_description);
    return step;
}

QueryPlanStepPtr QueryPlanStepHelper::fromProto(const Protos::ExtremesStep & proto_step, ContextPtr)
{
    auto [step_description, base_input_stream] = ProtosSerDerHelper::deserializeFromProtoBase(proto_step.query_plan_base());
    auto step = std::make_shared<ExtremesStep>(base_input_stream);
    step->setStepDescription(step_description);
    return step;
}

QueryPlanStepPtr QueryPlanStepHelper::fromProto(const Protos::ArrayJoinStep & proto_step, ContextPtr context)
{
    auto [step_description, base_input_stream] = ProtosSerDerHelper::deserializeFromProtoBase(proto_step.query_plan_base());
    auto array_join = ProtosSerDerHelper::fromProto(proto_step.array_join(), context);
    auto step = std::make_shared<ArrayJoinStep>(base_input_stream, array_join);
    step->setStepDescription(step_description);
    return step;
}

QueryPlanStepPtr QueryPlanStepHelper::fromProto(const Protos::WindowStep & proto_step, ContextPtr)
{
    auto [step_description, base_input_stream] = ProtosSerDerHelper::deserializeFromProtoBase(proto_step.query_plan_base());
    WindowDescription window_description = *ProtosSerDerHelper::fillFromProto(proto_step.window_description());
    std::vector<WindowFunctionDescription> window_functions;
    for (const auto & proto_element : proto_step.window_functions())
    {
        WindowFunctionDescription element = *ProtosSerDerHelper::fillFromProto(proto_element);
        window_functions.emplace_back(std::move(element));
    }
    auto streams_fan_out = proto_step.streams_fan_out();
    auto step = std::make_shared<WindowStep>(base_input_stream, window_description, window_functions, streams_fan_out);
    step->setStepDescription(step_description);
    return step;
}

#define FROM_PROTO_EXT_IMP(TYPE, VAR_NAME) \
QueryPlanStepPtr QueryPlanStepHelper::fromProto(const Protos::TYPE & proto, ContextPtr context) \
{ \
    return TYPE::fromProto(proto, context);\
}

APPLY_PROTOBUF_STEP_TYPES_AND_NAMES_FOR_EXT(FROM_PROTO_EXT_IMP)
#undef FROM_PROTO_EXT_IMP

QueryPlanStepPtr QueryPlanStepHelper::fromProto(const Protos::QueryPlanStep & proto, ContextPtr context)
{
    switch (proto.step_case())
    {
  // 1. StepExt with proto uses macros to execute fromProto, see PROTOBUF_STEP_TYPES_AND_NAMES_FOR_EXT
#define CASE_DEF(TYPE, VAR_NAME) \
    case Protos::QueryPlanStep::StepCase::k##TYPE: { \
        return fromProto(proto.VAR_NAME(), context); \
    }

        APPLY_PROTOBUF_STEP_TYPES_AND_NAMES_FOR_EXT(CASE_DEF)
#undef CASE_DEF

        // 2. Step with proto needs implementing fromProto manually, see PROTOBUF_STEP_TYPES_AND_NAMES
        case Protos::QueryPlanStep::StepCase::kFillingStep:
        {
            return fromProto(proto.filling_step(), context);
        }
        case Protos::QueryPlanStep::StepCase::kIntersectOrExceptStep:
        {
            return fromProto(proto.intersect_or_except_step(), context);
        }
        case Protos::QueryPlanStep::StepCase::kReadNothingStep:
        {
            return fromProto(proto.read_nothing_step(), context);
        }
        case Protos::QueryPlanStep::StepCase::kOffsetStep:
        {
            return fromProto(proto.offset_step(), context);
        }
        case Protos::QueryPlanStep::StepCase::kLimitByStep:
        {
            return fromProto(proto.limit_by_step(), context);
        }
        case Protos::QueryPlanStep::StepCase::kExtremesStep:
        {
            return fromProto(proto.extremes_step(), context);
        }
        case Protos::QueryPlanStep::StepCase::kArrayJoinStep:
        {
            return fromProto(proto.array_join_step(), context);
        }
        case Protos::QueryPlanStep::StepCase::kWindowStep:
        {
            return fromProto(proto.window_step(), context);
        }
        default: {
            throw Exception(ErrorCodes::PROTOBUF_BAD_CAST, "not implemented step: {}", static_cast<int>(proto.step_case()));
        }
    }
}

ActionsDAGPtr QueryPlanStepHelper::createFilterExpressionActions(ContextPtr context, const ASTPtr & filter, const Block & header)
{
    Names output;
    for (const auto & item : header)
        output.emplace_back(item.name);
    output.push_back(filter->getColumnName());

    return createExpressionActions(context, header.getNamesAndTypesList(), output, filter);
}

ActionsDAGPtr QueryPlanStepHelper::createExpressionActions(
    ContextPtr context, const NamesAndTypesList & source, const NamesWithAliases & output, const ASTPtr & ast, bool add_project)
{
    PreparedSetsPtr prepared_sets = std::make_shared<PreparedSets>();
    auto settings = context->getSettingsRef();
    SizeLimits size_limits_for_set(settings.max_rows_in_set, settings.max_bytes_in_set, settings.set_overflow_mode);
    auto actions = std::make_shared<ActionsDAG>(source);
    const NamesAndTypesList aggregation_keys;
    const ColumnNumbersList grouping_set_keys;
    ActionsVisitor::Data visitor_data(
        context,
        size_limits_for_set,
        0,
        source,
        std::move(actions),
        prepared_sets,
        true,
        false,
        false,
        {aggregation_keys, grouping_set_keys, GroupByKind::NONE});
    ActionsVisitor(visitor_data).visit(ast);
    actions = visitor_data.getActions();

    if (add_project)
        actions->project(output);
    else
        actions->addAliases(output);

    Names output_columns;
    for (const auto & item : output)
        if (!item.second.empty())
            output_columns.emplace_back(item.second);
        else
            output_columns.emplace_back(item.first);

    actions->removeUnusedActions(output_columns);

    return actions;
}

ActionsDAGPtr QueryPlanStepHelper::createExpressionActions(
    ContextPtr context, const NamesAndTypesList & source, const Names & output, const ASTPtr & ast, bool add_project)
{
    NamesWithAliases names_with_aliases;
    for (const auto & item : output)
        names_with_aliases.emplace_back(NameWithAlias{item, ""});

    return createExpressionActions(context, source, names_with_aliases, ast, add_project);
}

void QueryPlanStepHelper::projection(QueryPipelineBuilder & pipeline, const Block & target, const BuildQueryPipelineSettings & settings)
{
    if (!blocksHaveEqualStructure(pipeline.getHeader(), target))
    {
        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            pipeline.getHeader().getColumnsWithTypeAndName(), target.getColumnsWithTypeAndName(), ActionsDAG::MatchColumnsMode::Name);
        auto convert_actions = std::make_shared<ExpressionActions>(convert_actions_dag, settings.getActionsSettings());

        pipeline.addSimpleTransform([&](const Block & header) { return std::make_shared<ExpressionTransform>(header, convert_actions); });
    }
}

}
