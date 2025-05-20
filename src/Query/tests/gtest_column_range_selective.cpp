#include <iostream>
#include <string>
#include <gtest/gtest.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
// #include <Columns/ColumnBitMap64.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/logger_useful.h>


using namespace DB;

namespace UnitTest
{

/// compare InsertRangeSelective with following code :
/// for (size_t i = 0; i < length; i++)
///     insertFrom(src, selector[selector_start + i]);
/// Example: Selector 0 1 2 3 4 5 -> 3 4 5 0 1 2
static void compareInsertRangeSelectiveWithInsertFrom(IColumn & src_column, bool direct_compare_element = false)
{
    const size_t total_size = src_column.size();
    const size_t length = (total_size - 1) / 2 + 1; // (6-1) / 2 + 1 = 3
    auto dst_column = src_column.cloneEmpty();
    auto expect_column = src_column.cloneEmpty();
    IColumn::Selector selector(total_size, 0);
    for (size_t i = 0; i < total_size; i++)
    {
        auto partition_index = 1 - i / length;
        selector[i] = partition_index;
    }
    expect_column->insertManyFrom(src_column, length, total_size - length); // 3 4 5
    expect_column->insertManyFrom(src_column, 0, length);                   // 0 1 2

    //index is partition_index
    auto split_columns = src_column.scatter(2, selector);
    EXPECT_EQ(2, split_columns.size());
    dst_column->insertManyFrom(*split_columns[0], 0, split_columns[0]->size());
    dst_column->insertManyFrom(*split_columns[1], 0, split_columns[1]->size());

    EXPECT_EQ(expect_column->size(), dst_column->size());
    for (size_t i = 0; i < total_size; i++)
    {
        if (direct_compare_element)
        {
            EXPECT_EQ(true, (*expect_column)[i] == (*dst_column)[i]);
        }
        else
        {
            EXPECT_EQ(0, expect_column->compareAt(i, i, *dst_column, 1));
        }
    }
}

TEST(RangeSelectiveTest, ColumnArrayTest)
{
    size_t max_size = 10;
    auto val = ColumnUInt32::create();
    auto off = ColumnUInt64::create();
    auto & val_data = val->getData();
    auto & off_data = off->getData();

    /* [1]
     * [1, 1]
     * [1, 1, 1]
     * ...
     * [2]
     * [2, 2]
     * [2, 2, 2]
     * ...
     */
    UInt64 cur_off = 0;
    for (int idx [[maybe_unused]] : {1, 2})
    {
        UInt32 cur = 0;
        for (int64_t i = 0; i < 64; ++i)
        {
            size_t s = (i % max_size) + 1;

            cur_off += s;
            off_data.push_back(cur_off);

            for (size_t j = 0; j < s; ++j)
                val_data.push_back(cur);

            if (s == max_size)
                ++cur;
        }
    }

    auto src_column = ColumnArray::create(std::move(val), std::move(off));
    compareInsertRangeSelectiveWithInsertFrom(*src_column);
}

TEST(RangeSelectiveTest, ColumnConstTest)
{
    ColumnPtr nest_src_column = ColumnUInt64::create(1, 8);
    auto src_column = ColumnConst::create(nest_src_column, 100);
    compareInsertRangeSelectiveWithInsertFrom(*src_column);
}

TEST(RangeSelectiveTest, ColumnUInt64Test)
{
    const size_t size = 10;
    auto src_column = ColumnUInt64::create();

    for (size_t i = 0; i < size; i++)
    {
        src_column->insert(i * 10);
    }
    compareInsertRangeSelectiveWithInsertFrom(*src_column);
}

TEST(RangeSelectiveTest, ColumnFixedStringTest)
{
    const size_t size = 100;
    auto src_column = ColumnFixedString::create(100);
    for (size_t i = 0; i < size; i++)
    {
        src_column->insert("value: " + std::to_string(i));
    }
    compareInsertRangeSelectiveWithInsertFrom(*src_column);
}

TEST(RangeSelectiveTest, ColumnLowcardinalityTest)
{
    MutableColumnPtr src_column = DataTypeLowCardinality(std::make_shared<DataTypeUInt8>()).createColumn();
    for (size_t i = 0; i < 100; ++i)
    {
        src_column->insert(i);
    }
    compareInsertRangeSelectiveWithInsertFrom(*src_column);
}

TEST(RangeSelectiveTest, ColumnStringMapTest)
{
    const size_t size = 100;
    auto key_column = ColumnString::create();
    auto value_column = ColumnString::create();
    auto offset_column = ColumnVector<UInt64>::create();

    size_t offest_size = 0;
    for (size_t i = 0; i < size; i++)
    {
        key_column->insert("key: " + std::to_string(i));
        value_column->insert("value: " + std::to_string(i));
        if (i % 2 == 1)
        {
            offest_size += 2;
            offset_column->insert(offest_size);
        }
    }

    ColumnPtr src_column = ColumnMap::create(ColumnArray::create(
            ColumnTuple::create(Columns{std::move(key_column), std::move(value_column)}),
            std::move(offset_column)));

    compareInsertRangeSelectiveWithInsertFrom(const_cast<IColumn&>(*src_column), true);
}

TEST(RangeSelectiveTest, ColumnUInt64MapTest)
{
    const size_t size = 100;
    auto col1 = ColumnUInt64::create();
    auto col2 = ColumnUInt64::create();
    auto & data1 = col1->getData();
    auto & data2 = col2->getData();
    auto off = ColumnUInt64::create();
    auto & off_data = off->getData();

    size_t offest_size = 0;
    for (uint64_t i = 0; i < size; ++i)
    {
        offest_size++;
        data1.push_back(i);
        data2.push_back(i * 2);
        off_data.push_back(offest_size);
    }

    ColumnPtr src_column = ColumnMap::create(ColumnArray::create(
            ColumnTuple::create(Columns{std::move(col1), std::move(col2)}),
            std::move(off)));
    compareInsertRangeSelectiveWithInsertFrom(const_cast<IColumn&>(*src_column), true);
}

TEST(RangeSelectiveTest, ColumnStringTest)
{
    const size_t size = 100;
    auto src_column = ColumnString::create();
    for (size_t i = 0; i < size; i++)
    {
        src_column->insert("value: " + std::to_string(i));
    }
    compareInsertRangeSelectiveWithInsertFrom(*src_column);
}

TEST(RangeSelectiveTest, ColumnNothingTest)
{
    auto src_column = ColumnNothing::create(100);
    compareInsertRangeSelectiveWithInsertFrom(*src_column);
}

}
