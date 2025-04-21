#include <Interpreters/AggregateDescription.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

namespace Protos
{
class DataStream;
class NameAndTypePair;
class SortColumnDescription;
class FillColumnDescription;
}

class ProtosSerDerHelper
{
public:
    ProtosSerDerHelper() = default;
    ~ProtosSerDerHelper() = default;

    static void serializeToProtoBase(const ITransformingStep & step, Protos::ITransformingStep & proto);
    static std::pair<String, DataStream> deserializeFromProtoBase(const Protos::ITransformingStep & proto);

    static void toProto(const DataStream & data_stream, Protos::DataStream & proto);
    static void fillFromProto(DataStream & data_stream, const Protos::DataStream & proto);

    static void toProto(const NameAndTypePair & pair, Protos::NameAndTypePair & proto);
    static void fillFromProto(NameAndTypePair & pair, const Protos::NameAndTypePair & proto);

    static void toProto(const SortColumnDescription & sort_column_description, Protos::SortColumnDescription & proto);
    static void fillFromProto(SortColumnDescription & sort_column_description, const Protos::SortColumnDescription & proto);

    static void toProto(const FillColumnDescription & fill_column_description, Protos::FillColumnDescription & proto);
    static void fillFromProto(FillColumnDescription & fill_column_description, const Protos::FillColumnDescription & proto);

    static void toProto(const AggregateDescription & aggregate_description, Protos::AggregateDescription & proto);
    static void fillFromProto(AggregateDescription & aggregate_description, const Protos::AggregateDescription & proto);

    static void toProto(const InputOrderInfo & input_order_info, Protos::InputOrderInfo & proto);
    static std::shared_ptr<InputOrderInfo> fillFromProto(const Protos::InputOrderInfo & proto);

    static void toProto(
        const SortColumnDescriptionWithColumnIndex & sort_column_description_with_column_index,
        Protos::SortColumnDescriptionWithColumnIndex & proto);
    static void fillFromProto(
        SortColumnDescriptionWithColumnIndex & sort_column_description_with_column_index,
        const Protos::SortColumnDescriptionWithColumnIndex & proto);
};

}
