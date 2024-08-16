#include <Storages/Consensus/MergeTreeChangeData.h>


namespace DB
{

MergeTreeChangeData::MergeTreeChangeData()
{
    storage_type = StorageType::MergeTree;
}

void MergeTreeChangeData::serializeImpl(WriteBuffer & /*buffer*/)
{
    //writeIntBinary(primary_key_pos, buffer);
}

void MergeTreeChangeData::deserializeImpl(ReadBuffer & /*buffer*/)
{
    //readIntBinary(primary_key_pos, buffer);
}

void MergeTreeChangeData::dumpHeaderImpl(WriteBufferFromOwnString & /*out*/)
{
    //out << "primary key pos " << primary_key_pos << ",";
}

}
