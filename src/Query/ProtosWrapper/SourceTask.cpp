#include "SourceTask.h"

namespace DB
{

RSourceTaskFilter SourceTaskFilter::toProto() const
{
    RSourceTaskFilter proto;
    proto.set_index(index);
    proto.set_count(count);
    for (const auto & b : buckets)
    {
        proto.add_buckets(b);
    }
    return proto;
}

void SourceTaskFilter::fromProto(const RSourceTaskFilter & proto)
{
    index = proto.index();
    count = proto.count();
    buckets = std::set<Int64>();
    for (const auto b : proto.buckets())
    {
        buckets.insert(b);
    }
}

}
