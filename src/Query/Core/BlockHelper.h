#pragma once

#include <Core/Block.h>
#include <IO/WriteHelpers.h>



namespace DB
{

using NameToType = std::map<String, DataTypePtr>;

class BlockHelper
{
public:
    static NameSet getNameSet(const Block & block);
    static NameToType getNamesToTypes(const Block & block);
};

}

