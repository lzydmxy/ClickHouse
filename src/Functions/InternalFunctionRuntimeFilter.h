#pragma once

#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Columns/ColumnsNumber.h>

namespace DB
{
/**
 * internal function for optimizer
 */
class InternalFunctionRuntimeFilter : public IFunction
{
public:
    static constexpr auto name = "$runtimeFilter";

    static FunctionPtr create(ContextPtr /*context*/)
    {
        return std::make_shared<InternalFunctionRuntimeFilter>();
    }

    String getName() const override { return name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool isSuitableForConstantFolding() const override
    {
        return false;
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & /*arguments*/, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        // just used to let VirtualColumnUtils::prepareFilterBlockWithQuery handle runtime filters well
        auto result = ColumnConst::create(ColumnUInt8::create(/* size */ 1, /* value */ 1U), input_rows_count);
        return result;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override { return std::make_shared<DataTypeUInt8>(); }

    size_t getNumberOfArguments() const override { return 4; }
};

}
