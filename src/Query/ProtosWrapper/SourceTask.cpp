#include <Query/Executor/SourceTask.h>
#include "common/types.h"


namespace DB
{

RPCSourceTaskFilter SourceTaskFilter::toProto() const
{
    RSourceTaskFilter proto;
    if (index && count)
    {
        proto.set_index(index.value());
        proto.set_count(count.value());
    }
    else if (buckets)
    {
        for (const auto & b : buckets.value())
        {
            proto.add_buckets(b);
        }
    }
    return proto;
}

void SourceTaskFilter::fromProto(const RPCSourceTaskFilter & proto)
{
    if (proto.has_count() && proto.has_index())
    {
        index = proto.index();
        count = proto.count();
    }
    else
    {
        buckets = std::set<Int64>();
        for (const auto b : proto.buckets())
        {
            buckets->insert(b);
        }
    }
}

}
