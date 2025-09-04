#include <Functions/InternalFunctionRuntimeFilter.h>

#include <Functions/FunctionFactory.h>

namespace DB
{

void registerFunctionInternalFunctionRuntimeFilter(::DB::FunctionFactory & factory);

static ::DB::FunctionRegister REGISTER_FUNCTION_InternalFunctionRuntimeFilter(
    "InternalFunctionRuntimeFilter",
    registerFunctionInternalFunctionRuntimeFilter);

void registerFunctionInternalFunctionRuntimeFilter(::DB::FunctionFactory & factory)
{
    factory.registerFunction<InternalFunctionRuntimeFilter>();
}

}
