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

bool isIdentity(const String & symbol, const ConstASTPtr & expression) {
    return isIdentity(std::make_pair(symbol, expression));
}

bool isIdentity(const Assignment & assignment)
{
    String symbol = assignment.first;
    if (const auto * identifier = assignment.second->as<const ASTIdentifier>())
        return identifier->name() == symbol;
    return false;
}

bool isIdentity(const Assignments & assignments)
{
    return std::all_of(assignments.begin(), assignments.end(), [](const Assignment & assignment) {
        return isIdentity(assignment);
    });
}

bool isIdentity(const ProjectionStepExt & step)
{
    return !step.isFinalProject() && Utils::isIdentity(step.getAssignments());
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
