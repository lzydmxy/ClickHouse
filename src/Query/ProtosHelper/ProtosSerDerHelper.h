#include <Core/NamesAndTypes.h>
#include <Core/SortDescription.h>



namespace DB
{

namespace Protos
{
class NameAndTypePair;
class SortColumnDescription;
class FillColumnDescription;
}

class ProtosSerDerHelper
{
public:
    ProtosSerDerHelper() = default;
    ~ProtosSerDerHelper() = default;

    static void toProto(const NameAndTypePair & pair, Protos::NameAndTypePair & proto);
    static void fillFromProto(NameAndTypePair & pair, const Protos::NameAndTypePair & proto);

    static void toProto(const SortColumnDescription & pair, Protos::SortColumnDescription & proto);
    static void fillFromProto(SortColumnDescription & pair, const Protos::SortColumnDescription & proto);

    static void toProto(const FillColumnDescription & pair, Protos::FillColumnDescription & proto);
    static void fillFromProto(FillColumnDescription & pair, const Protos::FillColumnDescription & proto);

};

}
