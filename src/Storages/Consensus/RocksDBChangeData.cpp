#include <Storages/Consensus/RocksDBChangeData.h>


namespace DB
{

RocksDBChangeData::RocksDBChangeData()
{
    storage_type = StorageType::RocksDB;
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
