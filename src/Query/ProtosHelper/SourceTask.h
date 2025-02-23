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

/*

There structs is used for NodeSelector, must be in NodeSelector.h file.

struct SourceTaskPayload
{
    size_t rows = 0;
    size_t part_num = 0;
    std::set<Int64> buckets;
    String toString() const
    {
        return fmt::format("SourceTaskPayload(buckets:[{}],rows:{},part_num:{})",
            setToString<Int64>(buckets), rows, part_num);
    }
};

struct SourceTaskPayloadOnWorker
{
    String worker_id;
    size_t rows = 0;
    size_t part_num = 0;
    /// Bucket group is the minimum granularity to schedule source task, below is an example constructing bucket groups
    /// suppose we have two tables t1 and t2. t1's max bucket number is 4, t2's max bucket number is 8.
    /// t1 has buckets 0, 1, 2, 3 and t2 has buckets 0,1,2,3,4,5,6,7,8
    /// then we have bucket groups {0, 4}, {1, 5}, {2, 6}, {3, 7}
    std::map<Int64, std::set<Int64>> bucket_groups;
};
*/

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
