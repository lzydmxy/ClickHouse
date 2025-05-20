#pragma once
#include <Common/logger_useful.h>
#include <Columns/IColumn.h>

namespace DB
{

class ColumnSelector
{
public:
    static ColumnSelector & instance();
    void insertRangeSelective(MutableColumnPtr & target, const IColumn & src, const IColumn::Selector & selector, size_t selector_start, size_t length);
private:
    ColumnSelector();
    ~ColumnSelector() = default;
    LoggerPtr log;
    template <typename T>
    void numberRangeSelective(MutableColumnPtr & target, const IColumn & src, const IColumn::Selector & selector, size_t selector_start, size_t length);
};

}
