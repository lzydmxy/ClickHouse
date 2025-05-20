#pragma once

#include <Functions/FunctionHelpers.h>


namespace DB
{

String getFunctionResultName(const String & function_name, const Strings & arg_result_names);

}
