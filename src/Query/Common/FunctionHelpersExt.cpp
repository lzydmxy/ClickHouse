#include <Query/Common/FunctionHelpersExt.h>


namespace DB
{

String getFunctionResultName(const String & function_name, const Strings & arg_result_names)
{
    auto result_name = function_name + "(";
    for (size_t i = 0; i < arg_result_names.size(); ++i)
    {
        if (i)
            result_name += ", ";
        result_name += arg_result_names[i];
    }
    result_name += ")";
    return result_name;
}

}
