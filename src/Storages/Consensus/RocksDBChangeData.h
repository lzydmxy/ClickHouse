#pragma once

#include <Storages/Consensus/ChangeData.h>

namespace DB
{

class RocksDBChangeData : public ChangeData
{
public:
    RocksDBChangeData();
protected:
    void serializeImpl(WriteBuffer & buffer) override;
    void deserializeImpl(ReadBuffer & buffer) override;
    void dumpHeaderImpl(WriteBufferFromOwnString & out) override;
    size_t primary_key_pos;
};

}
