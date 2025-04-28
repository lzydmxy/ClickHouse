#pragma once

#include <IO/WriteBufferFromString.h>
#include <Query/Statistics/SerdeUtils.h>
#include <base/extended_types.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <base/wide_integer.h>

#include <serde.hpp>

namespace datasketches
{
// serde for all fixed-size arithmetic types (int and float of different sizes)
// in particular, kll_sketch<int64_t> should produce sketches binary-compatible
// with LongsSketch and ItemsSketch<Long> with ArrayOfLongsSerDe in Java

template <size_t Bits, typename Signed>
struct serde<wide::integer<Bits, Signed>>
{
    using T = wide::integer<Bits, Signed>;
    void serialize(std::ostream & os, const T * items, unsigned num) const
    {
        bool failure = false;
        try
        {
            os.write(reinterpret_cast<const char *>(items), sizeof(T) * num);
        }
        catch (std::ostream::failure &)
        {
            failure = true;
        }
        if (failure || !os.good())
        {
            throw std::runtime_error("error writing to std::ostream with " + std::to_string(num) + " items");
        }
    }
    void deserialize(std::istream & is, T * items, unsigned num) const
    {
        bool failure = false;
        try
        {
            is.read(reinterpret_cast<char *>(items), sizeof(T) * num);
        }
        catch (std::istream::failure &)
        {
            failure = true;
        }
        if (failure || !is.good())
        {
            throw std::runtime_error("error reading from std::istream with " + std::to_string(num) + " items");
        }
    }

    size_t size_of_item(const T &) const { return sizeof(T); }
    size_t serialize(void * ptr, size_t capacity, const T * items, unsigned num) const
    {
        const size_t bytes_written = sizeof(T) * num;
        check_memory_size(bytes_written, capacity);
        memcpy(ptr, items, bytes_written);
        return bytes_written;
    }
    size_t deserialize(const void * ptr, size_t capacity, T * items, unsigned num) const
    {
        const size_t bytes_read = sizeof(T) * num;
        check_memory_size(bytes_read, capacity);
        memcpy(items, ptr, bytes_read);
        return bytes_read;
    }
};
} /* namespace datasketches */

namespace std
{

template <size_t Bits, typename Signed>
String to_string(wide::integer<Bits, Signed> x)
{
    using T = wide::integer<Bits, Signed>;
    if constexpr (std::is_fundamental_v<T>)
    {
        return std::to_string(x);
    }
    else
    {
        DB::WriteBufferFromOwnString out;
        writeIntText(x, out);
        return std::move(out.str());
    }
}

template <size_t Bits, typename Signed>
void wideIntFromString(wide::integer<Bits, Signed> & result, const String & str)
{
    DB::ReadBufferFromString buf(str);
    DB::readIntText(result, buf);
}

template <size_t Bits, typename Signed>
ostream & operator<<(ostream & os, wide::integer<Bits, Signed> x)
{
    os << to_string(x);
    return os;
}

} /* namespace std */


namespace DB::QueryStatistics
{
template <typename T>
inline auto toDouble(T x)
{
    if constexpr (IsWideInteger<T>)
    {
        return static_cast<long double>(x);
    }
    else
    {
        return static_cast<double>(x);
    }
}

}
