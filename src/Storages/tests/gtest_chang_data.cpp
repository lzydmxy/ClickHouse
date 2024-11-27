#include <gtest/gtest.h>
#include <fstream>
#include <Storages/Consensus/ChangeData.h>
#include <Storages/Consensus/RocksDBChangeData.h>
#include <Storages/Consensus/ChangeDataCapture.h>
#include <Storages/Consensus/Settings.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <DataTypes/DataTypeFactory.h>
#include <Common/MatchGenerator.h>

#include <Coordination/ReadBufferFromNuraftBuffer.h>


using namespace DB;

TEST(ChangeData, ColumnStorage)
{
    auto settings = std::make_shared<Consensus::Settings>();
    settings->max_rows_for_row_store = 10000;
    ChangeDataPtr change_data = std::make_shared<RocksDBChangeData>(settings);

    change_data->storage_type = StorageType::RocksDB;
    change_data->op_num = RaftOpNum::Insert;

    auto block = std::make_shared<Block>();
    auto data_type = DataTypeFactory::instance().get("String");

    block->insert(ColumnWithTypeAndName{data_type->createColumn(), data_type, fmt::format("key") });
    for (UInt64 i = 0; i < 10; i++)
    {
        block->insert(ColumnWithTypeAndName{data_type->createColumn(), data_type, fmt::format("value{}",i) });
    }

    auto types = block->getDataTypes();
    auto columns = block->mutateColumns();

    auto default_format_settings = FormatSettings{};

    for (size_t i = 0; i < 20000; ++i)
    {
        size_t res_index = 0;
        columns[res_index++]->insert(fmt::format("NUM{}", i));
        for (size_t j = 0; j < 10; j++)
        {
            columns[res_index++]->insert(fmt::format("VALUE{}", j));
        }
    }

    for (size_t i = 0 ; i < columns.size(); i++)
    {
        auto ctn = block->getByPosition(i);
        block->setColumn(i, ColumnWithTypeAndName{std::move(columns[i]), ctn.type, ctn.name});
    }

    change_data->block = std::move(block);

    WriteBufferFromNuraftBuffer buf;
    change_data->serialize(buf);

    ReadBufferFromNuraftBuffer read_buf(buf.getBuffer());

    auto read_change_data= change_data->readFromBuffer(read_buf);

    const auto & columns_before = change_data->block->getColumns();
    const auto & columns_after = read_change_data->block->getColumns();


    ASSERT_EQ(change_data->block->rows(), read_change_data->block->rows());

    for (size_t i = 0; i < 11; ++i)
    {
        for (size_t j = 0; j < 20000; j++)
        {
            String before_value, after_value;
            (*columns_before[i])[j].tryGet(before_value);
            (*columns_after[i])[j].tryGet(after_value);

            ASSERT_EQ(before_value, after_value);
        }
    }
}


TEST(ChangeData, RowStorage)
{
    auto settings = std::make_shared<Consensus::Settings>();
    settings->max_rows_for_row_store = 100000;
    ChangeDataPtr change_data = std::make_shared<RocksDBChangeData>(settings);

    change_data->storage_type = StorageType::RocksDB;
    change_data->op_num = RaftOpNum::Insert;

    auto block = std::make_shared<Block>();
    auto data_type = DataTypeFactory::instance().get("String");

    block->insert(ColumnWithTypeAndName{data_type->createColumn(), data_type, fmt::format("key") });
    for (UInt64 i = 0; i < 10; i++)
    {
        block->insert(ColumnWithTypeAndName{data_type->createColumn(), data_type, fmt::format("value{}",i) });
    }

    auto types = block->getDataTypes();
    auto columns = block->mutateColumns();

    auto default_format_settings = FormatSettings{};

    for (size_t i = 0; i < 20000; ++i)
    {
        size_t res_index = 0;
        columns[res_index++]->insert(fmt::format("NUM{}", i));
        for (size_t j = 0; j < 10; j++)
        {
            columns[res_index++]->insert(fmt::format("VALUE{}", j));
        }
    }

    for (size_t i = 0 ; i < columns.size(); i++)
    {
        auto ctn = block->getByPosition(i);
        block->setColumn(i, ColumnWithTypeAndName{std::move(columns[i]), ctn.type, ctn.name});
    }

    change_data->block = std::move(block);

    WriteBufferFromNuraftBuffer buf;
    change_data->serialize(buf);

    ReadBufferFromNuraftBuffer read_buf(buf.getBuffer());

    auto read_change_data= change_data->readFromBuffer(read_buf);

    const auto & columns_before = change_data->block->getColumns();
    const auto & columns_after = read_change_data->block->getColumns();


    ASSERT_EQ(change_data->block->rows(), read_change_data->block->rows());

    for (size_t i = 0; i < 11; ++i)
    {
        for (size_t j = 0; j < 20000; j++)
        {
            String before_value, after_value;
            (*columns_before[i])[j].tryGet(before_value);
            (*columns_after[i])[j].tryGet(after_value);

            ASSERT_EQ(before_value, after_value);
        }
    }
}

