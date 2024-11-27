#pragma once

#include <Storages/Consensus/ChangeData.h>

namespace DB
{
class ReplicatedRocksDBBulkSink;

class RocksDBChangeData : public ChangeData
{
public:
    RocksDBChangeData(const SettingsPtr & settings_);
    RocksDBChangeData(const std::weak_ptr<ReplicatedRocksDBBulkSink> & rocks_sink_weak_ptr_, const SettingsPtr & settings_);
    std::weak_ptr<ReplicatedRocksDBBulkSink> rocks_sink_weak_ptr;
protected:
    void serializeImpl(WriteBuffer & buffer) override;
    void deserializeImpl(ReadBuffer & buffer) override;
    void dumpHeaderImpl(WriteBufferFromOwnString & out) override;
    size_t primary_key_pos;
};

}
