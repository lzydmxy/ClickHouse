#include <Query/Processors/QueryPlan/QueryPlanStepHelper.h>
#include <Query/ProtosHelper/FieldHelper.h>
#include <Query/ProtosHelper/PlanSerDerHelper.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>

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

}
