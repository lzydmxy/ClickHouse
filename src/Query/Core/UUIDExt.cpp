#include <Query/Core/UUIDExt.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace UUIDHelpers
{
String UUIDToString(const UUID & uuid)
{
    String uuid_str;
    WriteBufferFromString buff(uuid_str);
    writeUUIDText(uuid, buff);
    return uuid_str;
}

}
}
