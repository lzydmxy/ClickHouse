#include <Storages/Consensus/RocksDBChangeData.h>


namespace DB
{

RocksDBChangeData::RocksDBChangeData(const SettingsPtr & settings_): ChangeData(settings_)
{
    storage_type = StorageType::RocksDB;
}

RocksDBChangeData::RocksDBChangeData(const std::weak_ptr<ReplicatedRocksDBBulkSink> & rocks_sink_weak_ptr_, const SettingsPtr & settings_): RocksDBChangeData(settings_)
{
    rocks_sink_weak_ptr = rocks_sink_weak_ptr_;
}

void RocksDBChangeData::serializeImpl(WriteBuffer & buffer)
{
    writeIntBinary(primary_key_pos, buffer);
}

void RocksDBChangeData::deserializeImpl(ReadBuffer & buffer)
{
    readIntBinary(primary_key_pos, buffer);
}

void RocksDBChangeData::dumpHeaderImpl(WriteBufferFromOwnString & /*out*/)
{
    //out << "primary key pos " << primary_key_pos << ",";
}

}
