#pragma once

#include <IO/WriteBuffer.h>
#include <Core/Block.h>
#include <Storages/Consensus/NuRaft.h>
#include <Storages/Consensus/RaftCommon.h>
#include <Storages/MutationCommands.h>


namespace DB
{

using namespace Consensus;

enum class StorageType : int8_t
{
    NotSupport = 0,     /// Not support type
    RocksDB = 1,        /// RocksDB storage
    MergeTree = 2,      /// MergeTree storage
};

std::string toString(StorageType & type);

std::int8_t toInt8(StorageType storage_type);

StorageType getStorageType(int8_t storage_type_int);

StorageType matchType(const std::string & storage_name);

void write(StorageType x, WriteBuffer & out);

void read(StorageType & x, ReadBuffer & in);

class ChangeData;
using ChangeDataPtr = std::shared_ptr<ChangeData>;

const static std::uint8_t CDC_VERSION = 1;

using ConsumeCallback = std::function<void()>;

/*
* The base class for changing data of storages,
* used for data format conversion between storage and consistency protocols
*/
class ChangeData
{
public:
    ChangeData() : log(&Poco::Logger::get("ChangeData")) {}
    virtual ~ChangeData() {}
    void serialize(WriteBuffer & out);
    static ChangeDataPtr readFromBuffer(ReadBuffer & in);
    void merge(ChangeDataPtr data);
    std:: string dumpHeader();
public:
    StorageType storage_type {StorageType::NotSupport};
    std::uint8_t version {CDC_VERSION};
    std::string query;
    std::string database;
    std::string table;
    std::string leader_storage;
    RaftOpNum op_num {RaftOpNum::Error};
    MutationCommands commands;          //for delete
    BlockPtr block;
    std::string second_table;
    ConsumeCallback callback;
    StoragePtr target_table;
protected:
    void deserialize(ReadBuffer & in);
    void innerSerialize(WriteBuffer & out);
    virtual void dumpHeaderImpl(WriteBufferFromOwnString & out) = 0;
    virtual void serializeImpl(WriteBuffer & out) = 0;
    virtual void deserializeImpl(ReadBuffer & in) = 0;
    Poco::Logger * log;
};

using ChangeDatas = std::vector<ChangeDataPtr>;

}
