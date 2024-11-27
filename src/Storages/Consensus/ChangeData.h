#pragma once

#include <IO/WriteBuffer.h>
#include <Core/Block.h>
#include <Storages/Consensus/NuRaft.h>
#include <Storages/Consensus/RaftCommon.h>
#include <Storages/MutationCommands.h>
#include <Storages/Consensus/Settings.h>


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

enum class ChangeDataVersion : uint8_t
{
    V1 = 1, /// Row storage only
    V2 = 2, /// Support for row storage or column storage.
};

enum class ChangeDataStorageType : uint8_t
{
    ROW_STORE = 1,     /// Row storage
    COLUMN_STORE = 2,  /// Column storage
};

static constexpr auto CURRENT_CHANGE_DATA_VERSION = ChangeDataVersion::V2;

using ConsumeCallback = std::function<void()>;

/*
* The base class for changing data of storages,
* used for data format conversion between storage and consistency protocols
*/
class ChangeData
{
public:
    ChangeData(const SettingsPtr & settings_) : settings(settings_), log(&Poco::Logger::get("ChangeData")) {}
    virtual ~ChangeData() {}
    void serialize(WriteBuffer & out);
    static ChangeDataPtr readFromBuffer(ReadBuffer & in);
    void merge(ChangeDataPtr data);
    std:: string dumpHeader();
public:
    StorageType storage_type {StorageType::NotSupport};
    ChangeDataVersion version {CURRENT_CHANGE_DATA_VERSION};
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
    SettingsPtr settings;
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
