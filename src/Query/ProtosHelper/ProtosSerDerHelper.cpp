#include <Query/ProtosHelper/ProtosSerDerHelper.h>

#include <Query/ProtosHelper/DataTypeHelper.h>
#include <Query/ProtosHelper/FieldHelper.h>

#include <Query/Protos/plan_node.pb.h>


namespace DB
{

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
