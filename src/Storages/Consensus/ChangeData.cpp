#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <DataTypes/DataTypeFactory.h>
#include <Storages/Consensus/ChangeData.h>
#include <Storages/Consensus/RocksDBChangeData.h>
#include <Storages/Consensus/MergeTreeChangeData.h>
#include <IO/ReadBufferFromString.h>


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
    // 1.write version, storage_type, query, database and table
    writeIntBinary(version, out);
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

    LOG_DEBUG(log, "Serialize database {}, table {}, opnum {}, mutations {}, byte size {}, columns {}, rows {}",
        database, table, toString(op_num), commands.size(), cmd_buf.str().size(), block->columns(), block->rows());

    // 3.write column name and type
    writeVarUInt(static_cast<UInt64>(block->columns()), out);
    for (const auto & elem : *block)
    {
        writeStringBinary(elem.name, out);
        writeStringBinary(elem.type->getName(), out);
    }

    // 4.write data by row and column
    writeVarUInt(static_cast<UInt64>(block->rows()), out);
    for (size_t i = 0; i < block->rows(); ++i)
    {
        for (const auto & elem : *block)
            elem.type->getDefaultSerialization()->serializeBinary(*elem.column, i, out, {});
    }
}

ChangeDataPtr ChangeData::readFromBuffer(ReadBuffer & in)
{
    ChangeDataPtr data = nullptr;
    std::uint8_t version;
    readIntBinary(version, in);
    if (version != CDC_VERSION)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unknown ChangeData version {}", version);

    StorageType storage_type;
    read(storage_type, in);
    switch(storage_type)
    {
        case StorageType::RocksDB:
        {
            data = std::make_shared<RocksDBChangeData>();
            data->version = version;
            data->deserialize(in);
            return data;
        }
        case StorageType::MergeTree:
        {
            data = std::make_shared<MergeTreeChangeData>();
            data->version = version;
            data->deserialize(in);
            return data;
        }
        case StorageType::NotSupport:
            return data;
    }
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

    LOG_DEBUG(log, "Deserialize header database {}, table {}, opnum {}",
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

    //3. read column and type
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

    LOG_DEBUG(log, "Deserialize body commands {}, byte size {}, columns {}, read rows {}",
        commands.size(), cmd_str.size(), block->columns(), row_count);

    for (UInt64 i = 0; i < row_count; ++i)
    {
        for (size_t j = 0; j < columns.size(); j++)
        {
            Field val;
            types[j]->getDefaultSerialization()->deserializeBinary(val, in, {});
            columns[j]->insert(val);
        }
    }
    for (size_t i = 0 ; i < columns.size(); i++)
    {
        auto ctn = block->getByPosition(i);
        block->setColumn(i, ColumnWithTypeAndName{std::move(columns[i]), ctn.type, ctn.name});
    }
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
    out << "verion " << version << ",";
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
