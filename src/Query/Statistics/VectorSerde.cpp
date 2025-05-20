#include <DataTypes/IDataType.h>
#include <Query/Protos/optimizer_statistics.pb.h>
#include <Query/Statistics/SerdeUtils.h>
#include <Query/Statistics/TypeMacros.h>
#include <Common/Exception.h>

namespace DB::QueryStatistics
{
    template <typename T>
    std::string vectorSerialize(const std::vector<T> & data)
    {
        // todo: bc, optimize it to use better encoding
        static_assert(std::is_integral_v<T> || std::is_floating_point_v<T> || IsWideInteger<T>);
        static_assert(std::is_trivial_v<T>);
        // bool not supported due to stupid vector<bool>
        static_assert(!std::is_same_v<bool, T>);
        const char * ptr = reinterpret_cast<const char *>(data.data());
        auto bytes = sizeof(T) * data.size();
        return std::string(ptr, bytes);
    }

    template <typename T>
    std::vector<T> vectorDeserialize(std::string_view blob)
    {
        // todo: bc, optimize it to use better encoding
        // todo: bc, support string
        if (blob.size() % sizeof(T) != 0)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Corrupted Blob");
        }
        auto size = blob.size() / sizeof(T);
        std::vector<T> vec(size);
        memcpy(vec.data(), blob.data(), blob.size());
        return vec;
    }

#define INITIALIZE(TYPE) template std::string vectorSerialize<TYPE>(const std::vector<TYPE> & data);
FIXED_TYPE_ITERATE(INITIALIZE)
#undef INITIALIZE

#define INITIALIZE(TYPE) template std::vector<TYPE> vectorDeserialize<TYPE>(std::string_view blob);
FIXED_TYPE_ITERATE(INITIALIZE)
#undef INITIALIZE
}
