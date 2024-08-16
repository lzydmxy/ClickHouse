#pragma once

#include <Storages/Consensus/ChangeData.h>


namespace DB
{

class MergeTreeChangeData : public ChangeData
{
public:
    MergeTreeChangeData();
protected:
    void serializeImpl(WriteBuffer & buffer) override;
    void deserializeImpl(ReadBuffer & buffer) override;
    void dumpHeaderImpl(WriteBufferFromOwnString & out) override;
};

}
