#include <DataTypes/DataTypeFactory.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>
#include <Storages/Consensus/ChangeData.h>
#include <Storages/Consensus/RaftDispatcher.h>
#include <Storages/Consensus/MergeTreeChangeData.h>
#include <Storages/Consensus/RocksDBChangeData.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT_VERSION;
}

std::string toString(StorageType & type)
{
    switch (type)
    {
        case StorageType::RocksDB:
            return "RocksDB";
        case StorageType::MergeTree:
            return "MergeTree";
        case StorageType::NotSupport:
            return "NotSuppport";
    }
}

std::int8_t toInt8(StorageType storage_type)
{
    return static_cast<int8_t>(storage_type);
}

StorageType getStorageType(int8_t storage_type_int)
{
    return static_cast<StorageType>(storage_type_int);
}

StorageType matchType(const std::string & storage_name)
{
    if (storage_name.find("RocksDB") != std::string::npos)
        return StorageType::RocksDB;
    else if (storage_name.find("MergeTree") != std::string::npos)
        return StorageType::MergeTree;
    return StorageType::NotSupport;
}

void write(StorageType x, WriteBuffer & out)
{
    Consensus::write(static_cast<int8_t>(x), out);
}

void read(StorageType & x, ReadBuffer & in)
{
    int8_t raw_type;
    Consensus::read(raw_type, in);
    x = getStorageType(raw_type);
}

void ChangeData::serialize(WriteBuffer & out)
{
    innerSerialize(out);
}

void ChangeData::innerSerialize(WriteBuffer & out)
{
    LOG_DEBUG(log, "Serializing header database {}, table {}, opnum {}",
    database, table, toString(op_num));
    // 1.write version, storage_type, query, database and table
    writeIntBinary(static_cast<uint8_t>(version), out);
    write(storage_type, out);                //Create diffrent child change data object by storage_type
    writeStringBinary(query, out);
    writeStringBinary(database, out);
    writeStringBinary(table, out);
    writeStringBinary(leader_storage, out);
    write(op_num, out);
    writeStringBinary(second_table, out);

    serializeImpl(out);

    // 2. write mutation commands for delete only
    WriteBufferFromOwnString cmd_buf;
    if (op_num == RaftOpNum::Delete)
    {
        commands.writeText(cmd_buf, /* with_pure_metadata_commands = */ false);
        writeStringBinary(cmd_buf.str(), out);
    }

    auto row_count = block->rows();
    ChangeDataStorageType data_storage_type{ChangeDataStorageType::ROW_STORE};

    if (row_count > settings->max_rows_for_row_store)
    {
        data_storage_type = ChangeDataStorageType::COLUMN_STORE;
    }
    writeIntBinary(static_cast<uint8_t>(data_storage_type), out);

    LOG_DEBUG(log, "Serializing body, database {}, table {}, opnum {}, commands size {}, commands byte size {}, data storage type {}, columns {}, rows {}",
        database, table, toString(op_num), commands.size(), cmd_buf.str().size(), data_storage_type, block->columns(), block->rows());

    // 3.write column name and type
    writeVarUInt(static_cast<UInt64>(block->columns()), out);
    for (const auto & elem : *block)
    {
        writeStringBinary(elem.name, out);
        writeStringBinary(elem.type->getName(), out);
    }

    // 4.write data by row and column
    writeVarUInt(static_cast<UInt64>(row_count), out);

    if (data_storage_type == ChangeDataStorageType::COLUMN_STORE)
    {
        for (const auto & elem : *block)
            elem.type->getDefaultSerialization()->serializeBinaryBulk(*elem.column, out, 0, row_count);
    }
    else
    {
        for (size_t i = 0; i < row_count; ++i)
        {
            for (const auto & elem : *block)
                elem.type->getDefaultSerialization()->serializeBinary(*elem.column, i, out, {});
        }
    }

    LOG_DEBUG(log, "Serialized, database {}, table {}, opnum {}, commands size {}, commands byte size {}, data storage type {}, columns {}, rows {}",
    database, table, toString(op_num), commands.size(), cmd_buf.str().size(), data_storage_type, block->columns(), block->rows());
}

ChangeDataPtr ChangeData::readFromBuffer(ReadBuffer & in)
{
    ChangeDataPtr data = nullptr;
    std::uint8_t version;
    readIntBinary(version, in);
    auto change_data_version = static_cast<ChangeDataVersion>(version);
    if (change_data_version > CURRENT_CHANGE_DATA_VERSION)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unknown ChangeData version {}", version);

    StorageType storage_type;
    read(storage_type, in);

    Consensus::SettingsPtr settings;

    if (const auto & dispatcher = Context::getGlobalContextInstance()->getRaftDispatcher())
    {
        settings = dispatcher->getSettings();
    }

    if (!settings)
    {
        settings = std::make_shared<Consensus::Settings>();
    }

    switch(storage_type)
    {
        case StorageType::RocksDB:
        {
            data = std::make_shared<RocksDBChangeData>(std::move(settings));
            data->version = change_data_version;
            data->deserialize(in);
            break;
        }
        case StorageType::MergeTree:
        {
            data = std::make_shared<MergeTreeChangeData>(std::move(settings));
            data->version = change_data_version;
            data->deserialize(in);
            break;
        }
        case StorageType::NotSupport:
            break;
    }
    return data;
}

void ChangeData::deserialize(ReadBuffer & in)
{
    //after read version and storage_type
    readStringBinary(query, in);
    readStringBinary(database, in);
    readStringBinary(table, in);
    readStringBinary(leader_storage, in);
    read(op_num, in);
    readStringBinary(second_table, in);

    LOG_DEBUG(log, "Deserializing header, database {}, table {}, opnum {}",
        database, table, toString(op_num));

    //invoke child class method
    deserializeImpl(in);

    //2. read mutation commands, string buffer
    std::string cmd_str;
    if (op_num == RaftOpNum::Delete)
    {
        readStringBinary(cmd_str, in);
        ReadBufferFromOwnString cmd_buf{cmd_str};
        commands.readText(cmd_buf);
    }

    ChangeDataStorageType data_storage_type{ChangeDataStorageType::ROW_STORE};

    //3. read column and type
    if (version >= ChangeDataVersion::V2)
    {
        uint8_t data_storage_type_value;
        readIntBinary(data_storage_type_value, in);

        data_storage_type = static_cast<ChangeDataStorageType>(data_storage_type_value);
    }

    UInt64 column_count;
    readVarUInt(column_count, in);
    block = std::make_shared<Block>();
    for (UInt64 i = 0; i < column_count; i++)
    {
        std::string column_name, type_name;
        readStringBinary(column_name, in);
        readStringBinary(type_name, in);
        auto data_type = DataTypeFactory::instance().get(type_name);
        block->insert(ColumnWithTypeAndName{data_type->createColumn(), data_type, column_name});
    }

    //4. read data
    UInt64 row_count;
    readVarUInt(row_count, in);
    auto types = block->getDataTypes();
    auto columns = block->mutateColumns();

    LOG_DEBUG(log, "Deserializing body, database {}, table {}, opnum {}, commands size {}, data storage type {}, columns {}, rows {}",
    database, table, toString(op_num), commands.size(), data_storage_type, block->columns(), row_count);

    if (data_storage_type == ChangeDataStorageType::ROW_STORE)
    {
        FormatSettings default_format_settings{};
        for (UInt64 i = 0; i < row_count; ++i)
        {
            for (size_t j = 0; j < columns.size(); j++)
            {
                Field val;
                types[j]->getDefaultSerialization()->deserializeBinary(val, in, default_format_settings);
                columns[j]->insert(val);
            }
        }
        for (size_t i = 0 ; i < columns.size(); i++)
        {
            auto ctn = block->getByPosition(i);
            block->setColumn(i, ColumnWithTypeAndName{std::move(columns[i]), ctn.type, ctn.name});
        }
    }
    else
    {
        for (size_t i = 0; i < columns.size(); i++)
        {
            types[i]->getDefaultSerialization()->deserializeBinaryBulk(*columns[i], in, row_count, 0);
        }

        for (size_t i = 0 ; i < columns.size(); i++)
        {
            auto ctn = block->getByPosition(i);
            block->setColumn(i, ColumnWithTypeAndName{std::move(columns[i]), ctn.type, ctn.name});
        }
    }

    LOG_DEBUG(log, "Deserialized, database {}, table {}, opnum {}, commands size {}, data storage type {}, columns {}, rows {}",
    database, table, toString(op_num), commands.size(), data_storage_type, block->columns(), block->rows());
}

void ChangeData::merge(ChangeDataPtr data)
{
    auto columns = block->mutateColumns();
    auto source_columns = data->block->mutateColumns();
    for (size_t column_index = 0; column_index < columns.size(); column_index++)
    {
        columns[column_index]->insertFrom(*source_columns[column_index], 0);
    }

    for (size_t i = 0 ; i < columns.size(); i++)
    {
        auto ctn = block->getByPosition(i);
        block->setColumn(i, ColumnWithTypeAndName{std::move(columns[i]), ctn.type, ctn.name});
    }
}

std::string ChangeData::dumpHeader()
{
    WriteBufferFromOwnString out;
    out << "storage type " << storage_type << ",";
    out << "verion " << static_cast<int8_t>(version) << ",";
    out << "query " << query << ",";
    out << "database " << database << ",";
    out << "table " << table << ",";
    out << "leader storage " << leader_storage << ",";
    out << "opnum " << op_num << ",";
    out << "second table " << second_table << ",";

    dumpHeaderImpl(out);

    out << "commands " << commands.size() << ",";

    out << "columns " << block->columns() << ",";
    for (const auto & elem : *block)
    {
        out << "column " << elem.name << " "  << elem.type->getName() << ",";
    }
    out << "rows " << block->rows();
    return out.str();
}

}
