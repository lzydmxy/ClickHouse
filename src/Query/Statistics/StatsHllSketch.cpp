#include <Query/Statistics/StatsHllSketch.h>

namespace DB::QueryStatistics
{
    String StatsHllSketch::serialize() const
    {
        std::ostringstream ss;
        getFullResult().serialize_updatable(ss);
        return ss.str();
    }

    void StatsHllSketch::deserialize(std::string_view blob)
    {
        if (blob.empty())
        {
            data.reset();
            un_opt = std::nullopt;
            return;
        }
        data = decltype(data)::deserialize(blob.data(), blob.size());
        un_opt = std::nullopt;
    }

} // namespace DB
