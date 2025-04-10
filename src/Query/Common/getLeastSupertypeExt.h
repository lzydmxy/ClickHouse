#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

DataTypePtr getCommonType(const DataTypes & types, bool enable_implicit_arg_type_convert);
}
