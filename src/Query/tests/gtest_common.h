#pragma once
#include <cstddef>
#include <base/types.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Processors/Chunk.h>
#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Interpreters/Context_fwd.h>
#include <Functions/IFunction.h>

namespace UnitTest
{
extern DB::Chunk createUInt8Chunk(size_t row_num, size_t column_num, UInt8 value);

extern DB::Block createUInt64Block(size_t row_num, size_t column_num, UInt8 value);

extern DB::ExecutableFunctionPtr createRepartitionFunction(DB::ContextPtr context, const DB::ColumnsWithTypeAndName & arguments);

void setQueryDuration(DB::ContextMutablePtr context);

DB::ContextMutablePtr getInitContext();

}
