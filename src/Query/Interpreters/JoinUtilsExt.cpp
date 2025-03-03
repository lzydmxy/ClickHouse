#include <Query/Interpreters/JoinUtilsExt.h>

namespace DB
{

namespace JoinCommon
{

DataTypePtr tryConvertTypeToNullable(const DataTypePtr & type)
{
    if (canBecomeNullable(type))
        return convertTypeToNullable(type);
    return type;
}

}

}
