#include <memory>
#include <vector>
#include <gtest/gtest.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/logger_useful.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNothing.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNothing.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Processors/Chunk.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Query/tests/gtest_common.h>
#include <Query/Exchange/RepartitionTransform.h>

using namespace DB;
namespace UnitTest
{

size_t getPartitionNumber(IColumn::Selector & selector)
{
    std::set<size_t> partitions;
    for(size_t i = 0; i < selector.size(); i++)
    {
        if(partitions.find(i) == partitions.end())
            partitions.insert(selector[i]);
    }
    return partitions.size();
}

TEST(RepartitionTransformTest, doRepartitionTest)
{
    const size_t partition_num = 3;
    const size_t rows = 40;
    Block block = createUInt64PartitionBlock(rows, 3, 3);
    Block header = block.cloneEmpty();
    Chunk chunk(block.mutateColumns(), rows);
    ColumnsWithTypeAndName arguments;
    arguments.push_back(header.getByPosition(1));
    arguments.push_back(header.getByPosition(2));
    auto func = createRepartitionFunction(getContext().context, arguments);
    IColumn::Selector && selector = RepartitionTransform::doRepartition(
        partition_num, chunk, header, ColumnNumbers{1, 2}, func, RepartitionTransform::REPARTITION_FUNC_RESULT_TYPE);
    auto repartition_number = getPartitionNumber(selector);
    LOG_TRACE(getLogger("RepartitionTransformTest"), "Selector rows {}, re partition number {}, partition number {}", selector.size(), repartition_number, partition_num);
    ASSERT_TRUE(selector.size() == rows);
    ASSERT_TRUE(repartition_number > 0 && repartition_number <= partition_num);
}

TEST(RepartitionTransformTest, doRepartitionNullableTest)
{
    const size_t partition_num = 6;
    const size_t rows = 100;
    ColumnsWithTypeAndName cols;
    for (int i = 0; i < 3; i++)
    {
        auto nest_col = ColumnUInt64::create(rows, 88);
        auto res_null_map = ColumnUInt8::create(rows,1);
        auto column = ColumnNullable::create(std::move(nest_col), std::move(res_null_map));
        cols.emplace_back(std::move(column), std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "column" + std::to_string(i));
    }

    Block block {cols};
    Block header = block.cloneEmpty();
    Chunk chunk(block.mutateColumns(), rows);
    ColumnsWithTypeAndName arguments;
    arguments.push_back(header.getByPosition(1));
    arguments.push_back(header.getByPosition(2));
    auto func = createRepartitionFunction(getContext().context, arguments);
    auto selector  = RepartitionTransform::doRepartition(
        partition_num, chunk, header, ColumnNumbers{1, 2}, func, RepartitionTransform::REPARTITION_FUNC_RESULT_TYPE);
    auto repartition_number = getPartitionNumber(selector);
    LOG_TRACE(getLogger("RepartitionTransformTest"), "Selector rows {}, re partition number {}, partition number {}", selector.size(), repartition_number, partition_num);
    ASSERT_TRUE(selector.size() == rows);
    ASSERT_TRUE(repartition_number > 0 && repartition_number <= partition_num);
}

TEST(RepartitionTransformTest, doRepartitionOnlyNullTest)
{
    const size_t partition_num = 6;
    const size_t rows = 10;
    ColumnsWithTypeAndName cols;
    for (int i = 0; i < 3; i++)
    {
        auto nest_col = ColumnNothing::create(1);
        auto res_null_map = ColumnUInt8::create(1, 1);
        auto null_column = ColumnNullable::create(std::move(nest_col), std::move(res_null_map));
        auto column = ColumnConst::create(std::move(null_column), rows);
        cols.emplace_back(
            std::move(column), std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>()), "column" + std::to_string(i));
    }

    Block block{cols};

    Block header = block.cloneEmpty();
    Chunk chunk(block.mutateColumns(), rows);
    ColumnsWithTypeAndName arguments;
    arguments.push_back(header.getByPosition(1));
    arguments.push_back(header.getByPosition(2));
    auto func = createRepartitionFunction(getContext().context, arguments);
    EXPECT_THROW(RepartitionTransform::doRepartition(
        partition_num, chunk, header, ColumnNumbers{1, 2}, func, RepartitionTransform::REPARTITION_FUNC_RESULT_TYPE), DB::Exception);
    EXPECT_NO_THROW(RepartitionTransform::doRepartition(
        partition_num, chunk, header, ColumnNumbers{1, 2}, func, RepartitionTransform::REPARTITION_FUNC_NULLABLE_RESULT_TYPE));
}

}
