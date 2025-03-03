#include <Query/Common/Utils.h>

#include <Common/Exception.h>

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
}
