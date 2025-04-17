#include <Processors/QueryPlan/ITransformingStep.h>

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

    static void toProto(const SortColumnDescription & pair, Protos::SortColumnDescription & proto);
    static void fillFromProto(SortColumnDescription & pair, const Protos::SortColumnDescription & proto);

    static void toProto(const FillColumnDescription & pair, Protos::FillColumnDescription & proto);
    static void fillFromProto(FillColumnDescription & pair, const Protos::FillColumnDescription & proto);
};

}
