#pragma once

#include <Storages/IStorage_fwd.h>
#include <Core/UUID.h>

namespace DB
{

class ProjectionStep;
struct AggregateDescription;

namespace Utils
{

    void checkArgument(bool expression);
    void checkArgument(bool expression, const String & msg);

    void checkState(bool expression);
    void checkState(bool expression, const String & msg);

}

namespace UUIDHelpers
{

    String UUIDToString(const UUID & uuid);
}

}
