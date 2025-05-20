#include <Query/Common/getLeastSupertypeExt.h>

#include <DataTypes/getLeastSupertype.h>

#include <DataTypes/IDataType.h>
#include <IO/Operators.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NO_COMMON_TYPE;
}

DataTypePtr getCommonType(const DataTypes & types, bool enable_implicit_arg_type_convert, bool)
{
    if (enable_implicit_arg_type_convert)
        return getLeastSupertype<LeastSupertypeOnError::String>(types);
    else
        return getLeastSupertype(types);
}

}
