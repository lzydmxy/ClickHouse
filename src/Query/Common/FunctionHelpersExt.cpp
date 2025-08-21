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

bool isSuitableForConstantFoldingInOptimizer(const FunctionBasePtr & function_base)
{
    // todo: zhangwanyun1, now only support in
    // functions about ExternalDictionary, in, map will override and return false, adjustments will be made later if necessary
    static const std::unordered_set<std::string> unsuitable_function_names = {
        "in", "globalIn", "notIn", "globalNotIn", "nullIn", "globalNullIn",
        "notNullIn", "globalNotNullIn", "inIgnoreSet", "globalInIgnoreSet",
        "notInIgnoreSet", "globalNotInIgnoreSet", "nullInIgnoreSet", "globalNullInIgnoreSet",
        "notNullInIgnoreSet", "globalNotNullInIgnoreSet"
    };

    if (unsuitable_function_names.contains(function_base->getName()))
        return false;

    return function_base->isSuitableForConstantFolding();
}

}
