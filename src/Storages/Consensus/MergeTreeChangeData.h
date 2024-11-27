#pragma once

#include <Storages/Consensus/ChangeData.h>


namespace DB
{

class MergeTreeChangeData : public ChangeData
{
public:
    MergeTreeChangeData(const SettingsPtr & settings_);
protected:
    void serializeImpl(WriteBuffer & buffer) override;
    void deserializeImpl(ReadBuffer & buffer) override;
    void dumpHeaderImpl(WriteBufferFromOwnString & out) override;
};

}
