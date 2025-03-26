#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Query/Interpreters/JoinUtilsExt.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>


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

/// Convert column to nullable. If column LowCardinality or Const, convert nested column.
/// Returns nullptr if conversion cannot be performed.
ColumnPtr tryConvertColumnToNullable(ColumnPtr col)
{
    if (col->isSparse())
        col = recursiveRemoveSparse(col);

    if (isColumnNullable(*col) || col->canBeInsideNullable())
        return makeNullable(col);

    if (col->lowCardinality())
    {
        const ColumnLowCardinality & col_lc = assert_cast<const ColumnLowCardinality &>(*col);
        if (col_lc.nestedIsNullable())
            return col;
        else if (col_lc.nestedCanBeInsideNullable())
            return col_lc.cloneNullable();
    }
    else if (const ColumnConst * col_const = checkAndGetColumn<ColumnConst>(*col))
    {
        const auto & nested = col_const->getDataColumnPtr();
        if (nested->isNullable() || nested->canBeInsideNullable())
        {
            return makeNullable(col);
        }
        else if (nested->lowCardinality())
        {
            ColumnPtr nested_nullable = tryConvertColumnToNullable(nested);
            if (nested_nullable)
                return ColumnConst::create(nested_nullable, col_const->size());
        }
    }
    return nullptr;
}

bool isJoinCompatibleTypes(const DataTypePtr & left, const DataTypePtr & right)
{
    auto left_base = removeNullable(recursiveRemoveLowCardinality(left));
    auto right_base = removeNullable(recursiveRemoveLowCardinality(right));
    return left_base->equals(*right_base);
}

}

}
