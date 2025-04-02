#include "gtest_common.h"
#include <cstddef>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsHashing.h>
#include <Functions/IFunction.h>
#include <Processors/Chunk.h>
#include <Interpreters/Context.h>
#include <Common/tests/gtest_global_register.h>
#include <Common/tests/gtest_global_context.h>
#include <Query/Common/OptimizerContext.h>

namespace UnitTest
{

using namespace DB;

Chunk createUInt8Chunk(size_t row_num, size_t column_num, UInt8 value)
{
    Columns columns;
    for (size_t i = 0; i < column_num; i++)
    {
        auto col = ColumnUInt8::create(row_num, value);
        columns.emplace_back(std::move(col));
    }
    return Chunk(std::move(columns), row_num);
}

Block createUInt64Block(size_t row_num, size_t column_num, UInt8 value)
{
    ColumnsWithTypeAndName cols;
    for (size_t i = 0; i < column_num; i++)
    {
        auto column = ColumnUInt64::create(row_num, value);
        cols.emplace_back(std::move(column), std::make_shared<DataTypeUInt64>(), "column" + std::to_string(i));
    }
    return Block(cols);
}

Block createUInt64PartitionBlock(size_t row_num, size_t column_num, size_t partition_num)
{
    ColumnsWithTypeAndName cols;
    size_t range = row_num / partition_num + 1;
    for (size_t i = 0; i < column_num; i++)
    {
        auto column = ColumnUInt64::create();
        for(size_t j = 0; j < row_num; j ++)
        {
            column->insertValue((j+1) / range);
        }
        cols.emplace_back(std::move(column), std::make_shared<DataTypeUInt64>(), "column" + std::to_string(i));
    }
    return Block(cols);
}

ExecutableFunctionPtr createRepartitionFunction(ContextPtr context, const ColumnsWithTypeAndName & arguments)
{
    tryRegisterFunctions();
    const String repartition_func_name = "cityHash64";
    auto & factory = FunctionFactory::instance();
    auto res = factory.tryGetImpl(repartition_func_name, context);
    FunctionOverloadResolverPtr func_builder = factory.get(repartition_func_name, context);
    FunctionBasePtr function_base = func_builder->build(arguments);
    return function_base->prepare(arguments);
}

void setQueryDuration(DB::ContextMutablePtr context)
{
    auto & client_info = context->getClientInfo();

    const auto current_time = std::chrono::system_clock::now();
    client_info.initial_query_start_time = timeInSeconds(current_time);
    client_info.initial_query_start_time_microseconds = timeInMicroseconds(current_time);

    context->getOptimizerContext()->initQueryExpirationTimeStamp();
}

DB::ContextMutablePtr getInitContext()
{
    auto context = getContext().context;
    context->initializeOptimizerContext();
    return context;
}

}
