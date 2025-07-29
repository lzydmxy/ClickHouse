#pragma once

#include <Query/Statistics/StatisticsBase.h>
#include <Common/Exception.h>

#include <Query/Statistics/Base64.h>
#include <Query/Statistics/DataSketchesHelper.h>

#include <Query/Statistics/serde_extend.h>

namespace DB::QueryStatistics
{
class StatsHllSketch : public StatisticsBase
{
public:
    static constexpr auto default_lg_k = 12;
    static constexpr auto tag = StatisticsTag::HllSketch;
    StatsHllSketch() : data(default_lg_k) { }

    String serialize() const override;
    void deserialize(std::string_view blob) override;
    StatisticsTag getTag() const override { return tag; }

    double getEstimate() const { return getFullResult().get_estimate(); }

    // In StatsHllSketch.h
    template <typename T>
    void update(const T& value)
    {
        if constexpr (std::is_integral_v<T>)
        {
            if constexpr (std::is_signed_v<T>)
            {
                if constexpr (sizeof(T) <= 1)
                    data.update(static_cast<int8_t>(value));
                else if constexpr (sizeof(T) <= 2)
                    data.update(static_cast<int16_t>(value));
                else if constexpr (sizeof(T) <= 4)
                    data.update(static_cast<int32_t>(value));
                else
                    data.update(static_cast<int64_t>(value));
            }
            else
            {
                if constexpr (sizeof(T) <= 1)
                    data.update(static_cast<uint8_t>(value));
                else if constexpr (sizeof(T) <= 2)
                    data.update(static_cast<uint16_t>(value));
                else if constexpr (sizeof(T) <= 4)
                    data.update(static_cast<uint32_t>(value));
                else
                    data.update(static_cast<uint64_t>(value));
            }
        }
        else if constexpr (std::is_floating_point_v<T>)
        {
            if constexpr (std::is_same_v<T, float>)
                data.update(static_cast<float>(value));
            else
                data.update(static_cast<double>(value));
        }
        else
        {
            static_assert(std::is_trivial_v<T> || std::is_same_v<UUID, T>
                || std::is_same_v<IPv4, T> || std::is_same_v<IPv6, T>);
            data.update(&value, sizeof(value));
        }
    }

    // template <typename T>
    // void update(const T & value)
    // {
    //     if constexpr (std::is_arithmetic_v<T> || std::is_same_v<T, String>)
    //     {
    //         data.update(value);
    //     }
    //     else
    //     {
    //         static_assert(std::is_trivial_v<T> || std::is_same_v<UUID, T>);
    //         T v = value;
    //         data.update(&v, sizeof(v));
    //     }
    // }

    void merge(const StatsHllSketch & rhs)
    {
        if (!un_opt.has_value())
        {
            un_opt.emplace(default_lg_k);
        }
        auto & un = un_opt.value();
        un.update(rhs.data);

        if (rhs.un_opt.has_value())
        {
            un.update(rhs.un_opt->get_result());
        }
    }

private:
    datasketches::hll_sketch getFullResult() const
    {
        if (un_opt.has_value())
        {
            auto tmp_un = *un_opt;
            tmp_un.update(data);
            return tmp_un.get_result();
        }
        else
        {
            return data;
        }
    }

private:
    datasketches::hll_sketch data;
    std::optional<datasketches::hll_union> un_opt;
};

// transform ndv to integer, and make it no greater than count
inline UInt64 AdjustNdvWithCount(double ndv_estimate, UInt64 count)
{
    UInt64 int_ndv = std::llround(ndv_estimate);
    return std::min(int_ndv, count);
}

}
