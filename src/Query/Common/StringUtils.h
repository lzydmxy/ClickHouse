#pragma once
#include <string>
#include <cstring>
#include <cstddef>
#include <cstdint>
#include <set>
#include <type_traits>
#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptor/transformed.hpp>

namespace detail
{
    void parseSlowQuery(const std::string& query, size_t & pos);
    void convertCamelToSnake(std::string & orig_str);
}

inline void parseSlowQuery(const std::string& query, size_t & pos)
{
    detail::parseSlowQuery(query, pos);
}

inline void convertCamelToSnake(std::string & orig_str)
{
    detail::convertCamelToSnake(orig_str);
}

namespace compatibility
{
namespace v1
{
    /// Due to undetermin result of std::hash
    /// Here we introduce gcc9's implementation of std::hash for backward compatibility
    /// Implementation is copied from gcc/include/c++/9.3.0/bits/hash_bytes.h
    size_t hash(const std::string & s);
}

namespace v2
{
    /// Due to undetermin result of std::hash
    /// Here we introduce libcxx's implementation of std::hash for backward compatibility
    /// Implementation is copied from contrib/libcxx/include/utility
    size_t hash(const std::string & s);   
}

}

namespace DB
{
    template <typename T>
    std::string setToString(const std::set<T> & vals)
    {
        return boost::join(vals | boost::adaptors::transformed([](T v) { return std::to_string(v); }), ",");
    }
}
