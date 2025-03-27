#include <Query/Analyzer/function_utils.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Functions/FunctionFactory.h>
#include <Parsers/ASTSubquery.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

FunctionType getFunctionType(const ASTFunction & function, ContextPtr context)
{
    if (function.is_window_function)
        return FunctionType::WINDOW_FUNCTION;
    else if (AggregateFunctionFactory::instance().isAggregateFunctionName(function.name))
        return FunctionType::AGGREGATE_FUNCTION;
    else if (function.name == "grouping")
        return FunctionType::GROUPING_OPERATION;
    else if (functionIsInSubquery(function))
        return FunctionType::IN_SUBQUERY;
    else if (functionIsExistsSubquery(function))
        return FunctionType::EXISTS_SUBQUERY;
    else if (function.name == "lambda")
        return FunctionType::LAMBDA_EXPRESSION;
    else if (FunctionFactory::instance().tryGet(function.name, context))
        return FunctionType::FUNCTION;
    else
        return FunctionType::UNKNOWN;
}

ASTs getLambdaExpressionArguments(ASTFunction & lambda)
{
    if (lambda.arguments->children.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "lambda requires two arguments");

    const auto * lambda_args_tuple = lambda.arguments->children.at(0)->as<ASTFunction>();

    if (!lambda_args_tuple || lambda_args_tuple->name != "tuple")
        throw Exception(ErrorCodes::TYPE_MISMATCH, "First argument of lambda must be a tuple");

    return lambda_args_tuple->arguments->children;
}

ASTPtr getLambdaExpressionBody(ASTFunction & lambda)
{
    if (lambda.arguments->children.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "lambda requires two arguments");

    return lambda.arguments->children.at(1);
}

bool isComparisonFunction(const ASTFunction & function)
{
    return function.name == "equals" || function.name == "bitEquals" || function.name == "less" || function.name == "lessOrEquals"
        || function.name == "greater" || function.name == "greaterOrEquals";
}

bool functionIsInSubquery(const ASTFunction & function)
{
    return (function.name == "in" || function.name == "notIn" || function.name == "globalIn" || function.name == "globalNotIn"
            || function.name == "nullIn" || function.name == "globalNullIn" || function.name == "notNullIn"
            || function.name == "globalNotNullIn")
        && function.arguments->children.size() == 2 && function.arguments->children[1]->as<ASTSubquery>();
}

bool functionIsExistsSubquery(const ASTFunction & function)
{
    auto is_exist = [](const ASTFunction & func) {
        return func.name == "exists" && func.arguments->children.size() == 1 && func.arguments->children[0]->as<ASTSubquery>();
    };

    if (function.name == "not" && function.arguments->children.size() == 1)
    {
        if (auto * func = function.arguments->children[0]->as<ASTFunction>())
        {
            return is_exist(*func);
        }
    }

    return is_exist(function);
}

}
