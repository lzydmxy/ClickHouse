#pragma once

#include <Storages/IStorage_fwd.h>

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

}
