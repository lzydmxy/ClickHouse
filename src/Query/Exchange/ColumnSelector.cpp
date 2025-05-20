#include "ColumnSelector.h"
#include <Core/TypeId.h>
#include <Columns/ColumnVector.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ColumnSelector & ColumnSelector::instance()
{
    static ColumnSelector selector;
    return selector;
}

ColumnSelector::ColumnSelector() : log(getLogger("ColumnSelector"))
{
}

void ColumnSelector::insertRangeSelective(MutableColumnPtr & target, const IColumn & src, const IColumn::Selector & selector, size_t selector_start, size_t length)
{
    LOG_TRACE(log, "Name {}, Family name {}, type {}", src.getName(), src.getFamilyName(), src.getDataType());

    switch (src.getDataType())
    {
        // case TypeIndex::Int8:
        // case TypeIndex:Int16:
        // case TypeIndex:Int32:
        // case TypeIndex:Int64:
        // case TypeIndex::Int128:
        // case TypeIndex::Int256:
        // case TypeIndex::UInt8
        // case TypeIndex:UInt16:
        // case TypeIndex:UInt32:
        case TypeIndex::UInt64:
        // case TypeIndex::UInt128:
        // case TypeIndex::UInt256:
            this->numberRangeSelective<UInt64>(target, src, selector, selector_start, length);
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The type is not supported {}", src.getDataType());
    }
}

template <typename T>
void ColumnSelector::numberRangeSelective(MutableColumnPtr & target, const IColumn & src, const IColumn::Selector & selector, size_t selector_start, size_t length)
{
    target->insertManyFrom(src, selector_start, length);
}

}
