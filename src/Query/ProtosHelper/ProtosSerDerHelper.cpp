#include <Query/Processors/QueryPlan/QueryPlanStepHelper.h>
#include <Query/ProtosHelper/FieldHelper.h>
#include <Query/ProtosHelper/PlanSerDerHelper.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>
#include <Query/Core/FieldHelper.h>
#include <Query/ProtosHelper/RPCHelpers.h>

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

}
