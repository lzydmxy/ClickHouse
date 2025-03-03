#include <Query/Protos/EnumMacros.h>
#include <Common/Exception.h>

namespace DB
{
void throwBetterEnumException(const char * type, const char * enum_name, int code)
{
    throw Exception(DB::ErrorCodes::PROTOBUF_BAD_CAST, "Invalid {} value {} for type {}.", type, code, enum_name);
}
}
