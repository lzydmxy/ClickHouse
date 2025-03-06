#pragma once

#include <Interpreters/JoinUtils.h>

namespace DB
{

namespace JoinCommon
{

DataTypePtr tryConvertTypeToNullable(const DataTypePtr & type);

ColumnPtr tryConvertColumnToNullable(ColumnPtr col);

}

}
