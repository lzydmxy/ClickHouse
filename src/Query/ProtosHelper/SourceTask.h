#pragma once

#include <map>
#include <set>
#include <string>
#include <fmt/core.h>
#include <base/types.h>
#include <Query/Common/QueryCommon.h>
#include <Query/ProtosHelper/QueryProto.h>

namespace DB
{

struct SourceTaskFilter
{
    UInt32 index;
    UInt32 count;
    std::set<Int64> buckets;
    bool isValid() const
    {
        return (index > 0 && count > 0) || buckets.size() > 0;
    }
    RSourceTaskFilter toProto() const;
    void fromProto(const RSourceTaskFilter & proto);
    String toString() const
    {
        return fmt::format("SourceTaskFilter(idx:{}, cnt:{}, buckets:{})", index, count, containerToString<std::set<Int64>>(buckets));
    }
};

}
