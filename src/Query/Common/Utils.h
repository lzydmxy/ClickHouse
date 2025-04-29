#pragma once

#include <Query/Processors/QueryPlan/Assignment.h>
#include <Query/Processors/QueryPlan/PlanNode.h>
#include <Storages/IStorage_fwd.h>
#include <unordered_map>
#include <DataTypes/IDataType.h>
#include <Core/UUID.h>
#include <Core/DecimalFunctions.h>
#include <time.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CLOCK_GETTIME;
}

class ProjectionStep;
struct AggregateDescription;

namespace Utils
{

void checkArgument(bool expression);
void checkArgument(bool expression, const String & msg);

void checkState(bool expression);
void checkState(bool expression, const String & msg);

bool isIdentity(const String & symbol, const ConstASTPtr & expression);
bool isIdentity(const Assignment & assignment);
bool isIdentity(const Assignments & assignments);
bool isIdentity(const ProjectionStep & project);

}

namespace UUIDHelpers
{
String UUIDToString(const UUID & uuid);
}

inline DateTime64 nowSubsecondDt64(UInt32 scale)
{
    static constexpr Int32 fractional_scale = 9;

    timespec spec{};
    if (clock_gettime(CLOCK_REALTIME, &spec))
        throw ErrnoException(ErrorCodes::CANNOT_CLOCK_GETTIME, "Cannot clock_gettime.");

    DecimalUtils::DecimalComponents<DateTime64> components{spec.tv_sec, spec.tv_nsec};

    // clock_gettime produces subsecond part in nanoseconds, but decimalFromComponents fractional is scale-dependent.
    // Andjust fractional to scale, e.g. for 123456789 nanoseconds:
    //   if scale is  6 (miscoseconds) => divide by 9 - 6 = 3 to get 123456 microseconds
    //   if scale is 12 (picoseconds)  => multiply by abs(9 - 12) = 3 to get 123456789000 picoseconds
    const auto adjust_scale = fractional_scale - static_cast<Int32>(scale);
    if (adjust_scale < 0)
        components.fractional *= intExp10(std::abs(adjust_scale));
    else if (adjust_scale > 0)
        components.fractional /= intExp10(adjust_scale);
    return DecimalUtils::decimalFromComponents<DateTime64>(components, scale);
}
}
