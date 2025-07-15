#include <Query/Processors/QueryPlan/QueryPlanStepHelper.h>
#include <Query/ProtosHelper/FieldHelper.h>
#include <Query/ProtosHelper/PlanSerDerHelper.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>
#include <Query/Core/FieldHelper.h>
#include <Query/ProtosHelper/RPCHelpers.h>
#include <Interpreters/InternalTextLogsQueue.h>

namespace DB
{

void ProtosSerDerHelper::serializeToProtoBase(const ITransformingStep & step, Protos::ITransformingStep & proto)
{
    proto.set_step_description(step.step_description);
    if (step.input_streams.empty())
    {
        DataStream stream{.header = Block()};
        toProto(stream, *proto.mutable_input_stream());
    }
    else
    {
        toProto(step.input_streams.front(), *proto.mutable_input_stream());
    }
}

// return step_description and base_input_stream
std::pair<String, DataStream> ProtosSerDerHelper::deserializeFromProtoBase(const Protos::ITransformingStep & proto)
{
    auto step_description = proto.step_description();
    DataStream input_stream;
    fillFromProto(input_stream, proto.input_stream());
    return std::make_pair(std::move(step_description), std::move(input_stream));
}

void ProtosSerDerHelper::toProto(const DataStream & data_stream, Protos::DataStream & proto)
{
    serializeHeaderToProto(data_stream.header, *proto.mutable_header());
    std::sort(proto.mutable_distinct_columns()->begin(), proto.mutable_distinct_columns()->end());
    proto.set_has_single_port(data_stream.has_single_port);
    for (const auto & element : data_stream.sort_description)
        toProto(element, *proto.add_sort_description());
    proto.set_sort_scope(DataStreamSortScopeConverter::toProto(data_stream.sort_scope));
}

void ProtosSerDerHelper::fillFromProto(DataStream & data_stream, const Protos::DataStream & proto)
{
    data_stream.header = deserializeHeaderFromProto(proto.header());
    data_stream.has_single_port = proto.has_single_port();
    for (const auto & proto_element : proto.sort_description())
    {
        SortColumnDescription element;
        fillFromProto(element, proto_element);
        data_stream.sort_description.emplace_back(std::move(element));
    }
    data_stream.sort_scope = DataStreamSortScopeConverter::fromProto(proto.sort_scope());
}

void ProtosSerDerHelper::toProto(const NameAndTypePair & pair, Protos::NameAndTypePair & proto)
{
    proto.set_name(pair.name);
    serializeDataTypeToProto(pair.type, *proto.mutable_type());
    serializeDataTypeToProto(pair.type_in_storage, *proto.mutable_type_in_storage());
    if (pair.subcolumn_delimiter_position.has_value())
        proto.set_subcolumn_delimiter_position(pair.subcolumn_delimiter_position.value());
}

void ProtosSerDerHelper::fillFromProto(NameAndTypePair & pair, const Protos::NameAndTypePair & proto)
{
    pair.name = proto.name();
    pair.type = deserializeDataTypeFromProto(proto.type());
    pair.type_in_storage = deserializeDataTypeFromProto(proto.type_in_storage());
    if (proto.has_subcolumn_delimiter_position())
        pair.subcolumn_delimiter_position = proto.subcolumn_delimiter_position();
}

void ProtosSerDerHelper::toProto(const SortColumnDescription & sort_column_description, Protos::SortColumnDescription & proto)
{
    proto.set_column_name(sort_column_description.column_name);
    proto.set_direction(sort_column_description.direction);
    proto.set_nulls_direction(sort_column_description.nulls_direction);
    if (sort_column_description.collator)
        proto.mutable_collator()->set_locale(sort_column_description.collator->getLocale());
    proto.set_with_fill(sort_column_description.with_fill);
    toProto(sort_column_description.fill_description, *proto.mutable_fill_description());
}

void ProtosSerDerHelper::fillFromProto(SortColumnDescription & sort_column_description, const Protos::SortColumnDescription & proto)
{
    sort_column_description.column_name = proto.column_name();
    sort_column_description.direction = proto.direction();
    sort_column_description.nulls_direction = proto.nulls_direction();
    if (proto.has_collator())
        sort_column_description.collator = std::make_shared<Collator>(proto.collator().locale());
    sort_column_description.with_fill = proto.with_fill();
    fillFromProto(sort_column_description.fill_description, proto.fill_description());
}

void ProtosSerDerHelper::toProto(const FillColumnDescription & fill_column_description, Protos::FillColumnDescription & proto)
{
    FieldToProto(fill_column_description.fill_from, *proto.mutable_fill_from());
    FieldToProto(fill_column_description.fill_to, *proto.mutable_fill_to());
    FieldToProto(fill_column_description.fill_step, *proto.mutable_fill_step());
}

void ProtosSerDerHelper::fillFromProto(FillColumnDescription & fill_column_description, const Protos::FillColumnDescription & proto)
{
    FieldFillFromProto(fill_column_description.fill_from ,proto.fill_from());
    FieldFillFromProto(fill_column_description.fill_to ,proto.fill_to());
    FieldFillFromProto(fill_column_description.fill_step ,proto.fill_step());
}

void ProtosSerDerHelper::toProto(const AggregateDescription & aggregate_description, Protos::AggregateDescription & proto)
{
    serializeAggregateFunctionToProto(aggregate_description.function, aggregate_description.parameters, *proto.mutable_function());

    for (const auto & element : aggregate_description.arguments)
        proto.add_arguments(element);
    for (const auto & element : aggregate_description.argument_names)
        proto.add_argument_names(element);
    proto.set_column_name(aggregate_description.column_name);
    proto.set_mask_column(aggregate_description.mask_column);
}

void ProtosSerDerHelper::fillFromProto(AggregateDescription & aggregate_description, const Protos::AggregateDescription & proto)
{
    DataTypes arg_types;
    std::tie(aggregate_description.function, aggregate_description.parameters, arg_types)
        = deserializeAggregateFunctionFromProto(proto.function());
    (void)arg_types;

    for (const auto & element : proto.arguments())
        aggregate_description.arguments.emplace_back(element);
    for (const auto & element : proto.argument_names())
        aggregate_description.argument_names.emplace_back(element);
    aggregate_description.column_name = proto.column_name();
    aggregate_description.mask_column = proto.mask_column();
}

void ProtosSerDerHelper::toProto(const InputOrderInfo & input_order_info, Protos::InputOrderInfo & proto)
{
    for (const auto & element : input_order_info.sort_description_for_merging)
        toProto(element, *proto.add_sort_description_for_merging());
    proto.set_direction(input_order_info.direction);
}

std::shared_ptr<InputOrderInfo> ProtosSerDerHelper::fillFromProto(const Protos::InputOrderInfo & proto)
{
    SortDescription sort_description_for_merging;
    for (const auto & proto_element : proto.sort_description_for_merging())
    {
        SortColumnDescription element;
        fillFromProto(element, proto_element);
        sort_description_for_merging.emplace_back(std::move(element));
    }
    auto direction = proto.direction();
    auto res = std::make_shared<InputOrderInfo>(std::move(sort_description_for_merging), 0, direction, 0);

    return res;
}

void ProtosSerDerHelper::toProto(
    const SortColumnDescriptionWithColumnIndex & sort_column_description_with_column_index,
    Protos::SortColumnDescriptionWithColumnIndex & proto)
{
    toProto(sort_column_description_with_column_index.base, *proto.mutable_base());
    proto.set_column_number(sort_column_description_with_column_index.column_number);
}

void ProtosSerDerHelper::fillFromProto(
    SortColumnDescriptionWithColumnIndex & sort_column_description_with_column_index,
    const Protos::SortColumnDescriptionWithColumnIndex & proto)
{
    fillFromProto(sort_column_description_with_column_index.base, proto.base());
    sort_column_description_with_column_index.column_number = proto.column_number();
}

void ProtosSerDerHelper::toProto(const WindowFrame & window_frame, Protos::WindowFrame & proto)
{
    proto.set_is_default(window_frame.is_default);
    proto.set_type(WindowFrameTypeConverter::toProto(window_frame.type));
    proto.set_begin_type(WindowFrameBoundaryTypeConverter::toProto(window_frame.begin_type));
    toProto(window_frame.begin_offset, *proto.mutable_begin_offset());
    proto.set_begin_preceding(window_frame.begin_preceding);
    proto.set_end_type(WindowFrameBoundaryTypeConverter::toProto(window_frame.end_type));
    toProto(window_frame.end_offset, *proto.mutable_end_offset());
    proto.set_end_preceding(window_frame.end_preceding);
}

std::shared_ptr<WindowFrame> ProtosSerDerHelper::fillFromProto(const Protos::WindowFrame & proto)
{
    auto window_frame = std::make_shared<WindowFrame>();
    window_frame->is_default = proto.is_default();
    window_frame->type = WindowFrameTypeConverter::fromProto(proto.type());
    window_frame->begin_type = WindowFrameBoundaryTypeConverter::fromProto(proto.begin_type());
    window_frame->begin_offset = *fillFromProto(proto.begin_offset());
    window_frame->begin_preceding = proto.begin_preceding();
    window_frame->end_type = WindowFrameBoundaryTypeConverter::fromProto(proto.end_type());
    window_frame->end_offset = *fillFromProto(proto.end_offset());
    window_frame->end_preceding = proto.end_preceding();
    return window_frame;
}

void ProtosSerDerHelper::toProto(const WindowFunctionDescription & func, Protos::WindowFunctionDescription & proto)
{
    proto.set_column_name(func.column_name);
    serializeAggregateFunctionToProto(func.aggregate_function, func.function_parameters, func.argument_types, *proto.mutable_aggregate_function());

    for (const auto & element : func.argument_names)
        proto.add_argument_names(element);
}

std::shared_ptr<WindowFunctionDescription> ProtosSerDerHelper::fillFromProto(const Protos::WindowFunctionDescription & proto)
{
    auto func = std::make_shared<WindowFunctionDescription>();
    func->column_name = proto.column_name();
    std::tie(func->aggregate_function, func->function_parameters, func->argument_types) = deserializeAggregateFunctionFromProto(proto.aggregate_function());

    for (const auto & element : proto.argument_names())
        func->argument_names.emplace_back(element);
    return func;
}


void ProtosSerDerHelper::toProto(const WindowDescription & window_desc, Protos::WindowDescription & proto)
{
    proto.set_window_name(window_desc.window_name);
    for (const auto & element : window_desc.partition_by)
        toProto(element, *proto.add_partition_by());
    for (const auto & element : window_desc.order_by)
        toProto(element, *proto.add_order_by());
    for (const auto & element : window_desc.full_sort_description)
        toProto(element, *proto.add_full_sort_description());
    toProto(window_desc.frame, *proto.mutable_frame());
    for (const auto & element : window_desc.window_functions)
        toProto(element, *proto.add_window_functions());
}

std::shared_ptr<WindowDescription> ProtosSerDerHelper::fillFromProto(const Protos::WindowDescription & proto)
{
    auto window_desc = std::make_shared<WindowDescription>();
    window_desc->window_name = proto.window_name();
    for (const auto & proto_element : proto.partition_by())
    {
        SortColumnDescription element;
        fillFromProto(element, proto_element);
        window_desc->partition_by.emplace_back(std::move(element));
    }
    for (const auto & proto_element : proto.order_by())
    {
        SortColumnDescription element;
        fillFromProto(element, proto_element);
        window_desc->order_by.emplace_back(std::move(element));
    }
    for (const auto & proto_element : proto.full_sort_description())
    {
        SortColumnDescription element;
        fillFromProto(element, proto_element);
        window_desc->full_sort_description.emplace_back(std::move(element));
    }
    window_desc->frame = *fillFromProto(proto.frame());
    for (const auto & proto_element : proto.window_functions())
    {
        WindowFunctionDescription element = *fillFromProto(proto_element);
        window_desc->window_functions.emplace_back(std::move(element));
    }
    return window_desc;
}

void ProtosSerDerHelper::toProto(const Field & field, Protos::Field & proto)
{
    auto type = field.getType();
    auto proto_type = FieldTypeWhichConverter::toProto(type);
    proto.set_type(proto_type);
    WriteBufferFromOwnString buf;
    FieldHelper::writeFieldBinaryBlobImpl(field, type, buf);
    proto.set_blob(std::move(buf.str()));
}

std::shared_ptr<Field> ProtosSerDerHelper::fillFromProto(const Protos::Field & proto)
{
    auto field = std::make_shared<Field>();
    auto type = FieldTypeWhichConverter::fromProto(proto.type());
    ReadBufferFromString buf(proto.blob());
    FieldHelper::readFieldBinaryBlobImpl(*field, type, buf);
    return field;
}

void ProtosSerDerHelper::toProto(const SizeLimits & size_limits, Protos::SizeLimits & proto)
{
    proto.set_max_rows(size_limits.max_rows);
    proto.set_max_bytes(size_limits.max_bytes);
    proto.set_overflow_mode(OverflowModeConverter::toProto(size_limits.overflow_mode));
}

void ProtosSerDerHelper::fillFromProto(SizeLimits & size_limits, const Protos::SizeLimits & proto)
{
    size_limits.max_rows = proto.max_rows();
    size_limits.max_bytes = proto.max_bytes();
    size_limits.overflow_mode = OverflowModeConverter::fromProto(proto.overflow_mode());
}

void ProtosSerDerHelper::toProto(const SettingChange & setting_change, Protos::SettingChange & proto)
{
    proto.set_name(setting_change.name);
    toProto(setting_change.value, *proto.mutable_value());
}

void ProtosSerDerHelper::fillFromProto(SettingChange & setting_change, const Protos::SettingChange & proto)
{
    setting_change.name = proto.name();
    setting_change.value = *fillFromProto(proto.value());
}

void ProtosSerDerHelper::toProto(const SettingsChanges & settings_changes, Protos::SettingsChanges & proto)
{
    for (const auto & element : settings_changes)
        toProto(element, *proto.add_settings_changes());
}

void ProtosSerDerHelper::fillFromProto(SettingsChanges & settings_changes, const Protos::SettingsChanges & proto)
{
    for (const auto & proto_element : proto.settings_changes())
    {
        SettingChange element;
        fillFromProto(element, proto_element);
        settings_changes.emplace_back(std::move(element));
    }
}

void ProtosSerDerHelper::toProto(const StorageID & storage_id, Protos::StorageID & proto)
{
    RPCHelpers::fillStorageID(storage_id, proto);
}

std::shared_ptr<StorageID> ProtosSerDerHelper::fromProto(const Protos::StorageID & proto, ContextPtr context)
{
    auto storage_id = std::make_shared<StorageID>(proto.database(), proto.table(), RPCHelpers::createUUID(proto.uuid()));

    if (!storage_id)
    {
        return std::make_shared<StorageID>("_dummy", "_dummy", UUID{});
    }

    // todo: liyang453, other feat: StorageID do not have server_vw_name in 24.3, may need to be added later
    // patch
    /*
    StoragePtr storage = DatabaseCatalog::instance().getTable(*storage_id, context);
    if (storage)
    {
        auto patched_storage_id = storage->getStorageID();

         set vw_name twice
        if (!proto.server_vw_name().empty())
            patched_storage_id.server_vw_name = proto.server_vw_name();
        return patched_storage_id;
    }
    */

    return storage_id;
}

std::shared_ptr<StorageID> ProtosSerDerHelper::tryFromProto(const Protos::StorageID & proto, ContextPtr context)
{
    try {
        return fromProto(proto, context);
    } catch (Exception &) {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        return std::make_shared<StorageID>();
    }
}

void ProtosSerDerHelper::serializeToProtoBase(const ISourceStep & step, Protos::ISourceStep & proto)
{
    serializeHeaderToProto(step.getOutputStream().header, *proto.mutable_output_header());
}

Block ProtosSerDerHelper::deserializeFromProtoBase(const Protos::ISourceStep & proto)
{
    Block output_header = deserializeHeaderFromProto(proto.output_header());
    return output_header;
}


void ProtosSerDerHelper::toProto(const Aggregator::Params & agg_params, Protos::AggregatorParams & proto)
{
    for (const auto & element : agg_params.keys)
        proto.add_keys(element);
    for (const auto & element : agg_params.aggregates)
        toProto(element, *proto.add_aggregates());
    proto.set_overflow_row(agg_params.overflow_row);
    proto.set_max_rows_to_group_by(agg_params.max_rows_to_group_by);
    proto.set_group_by_overflow_mode(OverflowModeConverter::toProto(agg_params.group_by_overflow_mode));
    proto.set_group_by_two_level_threshold(agg_params.group_by_two_level_threshold);
    proto.set_group_by_two_level_threshold_bytes(agg_params.group_by_two_level_threshold_bytes);
    proto.set_max_bytes_before_external_group_by(agg_params.max_bytes_before_external_group_by);
    proto.set_empty_result_for_aggregation_by_empty_set(agg_params.empty_result_for_aggregation_by_empty_set);
    proto.set_max_threads(agg_params.max_threads);
    proto.set_min_free_disk_space(agg_params.min_free_disk_space);
    proto.set_compile_aggregate_expressions(agg_params.compile_aggregate_expressions);
    proto.set_min_count_to_compile_aggregate_expression(agg_params.min_count_to_compile_aggregate_expression);
    proto.set_max_block_size(agg_params.max_block_size);
    proto.set_only_merge(agg_params.only_merge);
    proto.set_enable_prefetch(agg_params.enable_prefetch);
    proto.set_optimize_group_by_constant_keys(agg_params.optimize_group_by_constant_keys);
    proto.set_min_hit_rate_to_use_consecutive_keys_optimization(agg_params.min_hit_rate_to_use_consecutive_keys_optimization);
}

Aggregator::Params ProtosSerDerHelper::fromProto(const Protos::AggregatorParams & proto, ContextPtr context)
{
    Names keys;
    for (const auto & element : proto.keys())
        keys.emplace_back(element);

    AggregateDescriptions aggregates;
    for (const auto & proto_element : proto.aggregates())
    {
        AggregateDescription element;
        fillFromProto(element, proto_element);
        aggregates.emplace_back(std::move(element));
    }

    return Aggregator::Params(keys, aggregates, proto.overflow_row(), proto.max_rows_to_group_by(), OverflowModeConverter::fromProto(proto.group_by_overflow_mode()),
        proto.group_by_two_level_threshold(), proto.group_by_two_level_threshold_bytes(), proto.max_bytes_before_external_group_by(), proto.empty_result_for_aggregation_by_empty_set(),
        context ? context->getTempDataOnDisk() : nullptr, proto.max_threads(), proto.min_free_disk_space(), proto.compile_aggregate_expressions(),
        proto.min_count_to_compile_aggregate_expression(), proto.max_block_size(), proto.enable_prefetch(), proto.only_merge(),
        proto.optimize_group_by_constant_keys(), proto.min_hit_rate_to_use_consecutive_keys_optimization(), {});

}

void ProtosSerDerHelper::toProto(const ArrayJoinAction & array_join_action, Protos::ArrayJoinAction & proto)
{
    for (const auto & element : array_join_action.columns)
        proto.add_columns(element);
    std::sort(proto.mutable_columns()->begin(), proto.mutable_columns()->end());
    proto.set_is_left(array_join_action.is_left);
}

std::shared_ptr<ArrayJoinAction> ProtosSerDerHelper::fromProto(const Protos::ArrayJoinAction & proto, ContextPtr context)
{
    std::unordered_set<String> columns;
    for (const auto & element : proto.columns())
        columns.emplace(element);
    auto is_left = proto.is_left();
    auto step = std::make_shared<ArrayJoinAction>(columns, is_left, context);

    return step;
}

void ProtosSerDerHelper::toProto(const SelectQueryInfo & select_query_info, Protos::SelectQueryInfo & proto)
{
    serializeASTToProto(select_query_info.query, *proto.mutable_query());
    serializeASTToProto(select_query_info.view_query, *proto.mutable_view_query());
    // serializeASTToProto(select_query_info.partition_filter, *proto.mutable_partition_filter());
    // cache_info.toProto(*proto.mutable_cache_info());
    if (select_query_info.input_order_info)
        toProto(*select_query_info.input_order_info, *proto.mutable_input_order_info());
}

void ProtosSerDerHelper::fillFromProto(SelectQueryInfo & select_query_info, const Protos::SelectQueryInfo & proto)
{
    select_query_info.query = deserializeASTFromProto(proto.query());
    select_query_info.view_query = deserializeASTFromProto(proto.view_query());
    //select_query_info.partition_filter = deserializeASTFromProto(proto.partition_filter());
    select_query_info.input_order_info = proto.has_input_order_info() ? fillFromProto(proto.input_order_info()) : nullptr;
    //select_query_info.cache_info.fillFromProto(proto.cache_info());
}

void ProtosSerDerHelper::toProto(const Block & log_block, Protos::SendLogsRequest & request)
{
    if (log_block.rows() == 0)
        return;

    // Verify block structure matches expected log format
    const auto & sample_block = InternalTextLogsQueue::getSampleBlock();
    if (!blocksHaveEqualStructure(sample_block, log_block))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Log block structure doesn't match expected format");
    }

    const auto & columns = log_block.getColumns();
    size_t rows = log_block.rows();

    // Extract column data
    const auto & event_time_col = columns[0];           // event_time (DateTime)
    const auto & event_time_us_col = columns[1];        // event_time_microseconds (UInt32)
    const auto & host_name_col = columns[2];            // host_name (String)
    const auto & query_id_col = columns[3];             // query_id (String)
    const auto & thread_id_col = columns[4];            // thread_id (UInt64)
    const auto & priority_col = columns[5];             // priority (Int8)
    const auto & source_col = columns[6];               // source (String)
    const auto & text_col = columns[7];                 // text (String)

    // Convert each row to protobuf LogEntry
    for (size_t i = 0; i < rows; ++i)
    {
        auto * log_entry = request.add_logs();

        log_entry->set_event_time(event_time_col->getUInt(i));
        log_entry->set_event_time_microseconds(event_time_us_col->getUInt(i));
        log_entry->set_host_name(host_name_col->getDataAt(i).toString());
        log_entry->set_query_id(query_id_col->getDataAt(i).toString());
        log_entry->set_thread_id(thread_id_col->getUInt(i));
        log_entry->set_priority(static_cast<int32_t>(priority_col->getInt(i)));
        log_entry->set_source(source_col->getDataAt(i).toString());
        log_entry->set_text(text_col->getDataAt(i).toString());
    }
}

void ProtosSerDerHelper::fillFromProto(Block & log_block, const Protos::SendLogsRequest & request)
{
    MutableColumns log_columns = log_block.cloneEmptyColumns();

    for (const auto & log_entry : request.logs())
    {
        log_columns[0]->insert(log_entry.event_time());
        log_columns[1]->insert(log_entry.event_time_microseconds());
        log_columns[2]->insert(log_entry.host_name());
        log_columns[3]->insert(log_entry.query_id());
        log_columns[4]->insert(log_entry.thread_id());
        log_columns[5]->insert(static_cast<Int8>(log_entry.priority()));
        log_columns[6]->insert(log_entry.source());
        log_columns[7]->insert(log_entry.text());
    }

    log_block.setColumns(std::move(log_columns));
}

}
