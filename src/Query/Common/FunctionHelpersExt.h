#pragma once

#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>


namespace DB
{

String getFunctionResultName(const String & function_name, const Strings & arg_result_names);

bool isSuitableForConstantFoldingInOptimizer(const FunctionBasePtr & function_base);

}
