#pragma once

#include <AggregateFunctions/Helpers.h>

namespace DB
{
struct Settings;

/** Create an aggregate function with a numeric type in the template parameter, depending on the type of the argument.
  */
template <template <typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction * createWithNumericTypeOrDateOrDateTime(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<TYPE>(std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Enum8) return new AggregateFunctionTemplate<Int8>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Enum16) return new AggregateFunctionTemplate<Int16>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Date) return new AggregateFunctionTemplate<UInt16>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::DateTime) return new AggregateFunctionTemplate<UInt32>(std::forward<TArgs>(args)...);
    return nullptr;
}

}
