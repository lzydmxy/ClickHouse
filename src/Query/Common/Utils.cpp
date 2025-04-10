#include <Query/Common/Utils.h>

#include <Common/Exception.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Utils
{

void checkArgument(bool expression)
{
    if (!expression)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal Argument");
    }
}

void checkArgument(bool expression, const String & msg)
{
    if (!expression)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal Argument: {}", msg);
    }
}

void checkState(bool expression)
{
    if (!expression)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal State");
    }
}

void checkState(bool expression, const String & msg)
{
    if (!expression)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal State: {}", msg);
    }
}

}

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
