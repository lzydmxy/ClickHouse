#include <Query/Statistics/StatisticsBaseImpl.h>

#include <Query/Optimizer/Dump/ProtoEnumUtils.h>
#include <Query/Statistics/StatsColumnBasic.h>
#include <Query/Statistics/StatsDummy.h>
#include <Query/Statistics/StatsHllSketch.h>
#include <Query/Statistics/StatsKllSketchImpl.h>
#include <Query/Statistics/StatsNdvBucketsExtendImpl.h>
#include <Query/Statistics/StatsNdvBucketsImpl.h>
#include <Query/Statistics/StatsTableBasic.h>
#include <Query/Statistics/StatsUdiCounter.h>
#include <Query/Statistics/TypeMacros.h>
#include <Poco/JSON/Parser.h>
#include <Common/Exception.h>
#include <base/types.h>


namespace DB::QueryStatistics
{
template <class StatsType>
std::shared_ptr<StatsType> createStatisticsUntyped(StatisticsTag tag, std::string_view blob)
{
    static_assert(std::is_base_of_v<StatisticsBase, StatsType>);
    checkTag<StatsType>(tag);
    std::shared_ptr<StatsType> ptr = std::make_shared<StatsType>();
    ptr->deserialize(blob);
    return ptr;
}

template <class StatsType, typename T>
std::shared_ptr<StatsType> createStatisticsTypedImpl(StatisticsTag tag, std::string_view blob)
{
    static_assert(std::is_base_of_v<StatisticsBase, StatsType>);
    checkTag<StatsType>(tag);
    using ImplType = typename StatsType::template Impl<T>;
    auto ptr = std::make_shared<ImplType>();
    ptr->deserialize(blob);
    return ptr;
}

#define UNTYPED_INSTANTIATION(STATS_TYPE) \
    template std::shared_ptr<Stats##STATS_TYPE> createStatisticsUntyped<Stats##STATS_TYPE>(StatisticsTag, std::string_view blob);
UNTYPED_STATS_ITERATE(UNTYPED_INSTANTIATION)
#undef UNTYPED_INSTANTIATION

#define TYPED_INSTANTIATION(STATS_TYPE) \
    template std::shared_ptr<Stats##STATS_TYPE> createStatisticsTyped<Stats##STATS_TYPE>(StatisticsTag, std::string_view blob);
TYPED_STATS_ITERATE(TYPED_INSTANTIATION)
#undef TYPED_INSTANTIATION

template <class StatsType>
std::shared_ptr<StatsType> createStatisticsTyped(StatisticsTag tag, std::string_view blob)
{
    if (blob.size() < sizeof(SerdeDataType))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "statistics blob corrupted");
    }

    auto type_blob = blob.substr(0, sizeof(SerdeDataType));
    SerdeDataType serde_data_type;
    memcpy(&serde_data_type, type_blob.data(), type_blob.size());
    switch (serde_data_type)
    {
#define ENUM_CASE(TYPE) \
    case SerdeDataType::TYPE: { \
        return createStatisticsTypedImpl<StatsType, TYPE>(tag, blob); \
    }
        ALL_TYPE_ITERATE(ENUM_CASE)
#undef ENUM_CASE
        // NOTE: for compatibility, old version enum value is also supported
        case SerdeDataType::StringOldVersion:
            return createStatisticsTypedImpl<StatsType, String>(tag, blob);

        default: {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "unknown type");
        }
    }
}
template <class StatsType>
std::shared_ptr<StatsType> createStatisticsUntypedJson(StatisticsTag tag, std::string_view blob)
{
    static_assert(std::is_base_of_v<StatisticsBase, StatsType>);
    checkTag<StatsType>(tag);
    std::shared_ptr<StatsType> ptr = std::make_shared<StatsType>();
    ptr->deserializeFromJson(blob);
    return ptr;
}
template <class StatsType, typename T>
std::shared_ptr<StatsType> createStatisticsTypedJsonImpl(StatisticsTag tag, std::string_view blob)
{
    static_assert(std::is_base_of_v<StatisticsBase, StatsType>);
    checkTag<StatsType>(tag);
    using ImplType = typename StatsType::template Impl<T>;
    auto ptr = std::make_shared<ImplType>();
    ptr->deserializeFromJson(blob);
    return ptr;
}
template <class StatsType>
std::shared_ptr<StatsType> createStatisticsTypedJson(StatisticsTag tag, std::string_view blob)
{
    if (blob.size() < sizeof(SerdeDataType))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "statistics blob corrupted");
    }
    Poco::JSON::Object::Ptr json_object
        = Poco::JSON::Parser().parse(std::string{blob.data(), blob.size()}).extract<Poco::JSON::Object::Ptr>();

    Poco::JSON::Object::Ptr object_bounds_blob = json_object->getObject("bounds_blob");

    String type_str = object_bounds_blob->getValue<String>("type_id");
    SerdeDataType type_index = ProtoEnumUtils::serdeDataTypeFromString(type_str);

    switch (type_index)
    {
#define ENUM_CASE(TYPE) \
    case SerdeDataType::TYPE: { \
        return createStatisticsTypedJsonImpl<StatsType, TYPE>(tag, blob); \
    }
        ALL_TYPE_ITERATE(ENUM_CASE)
#undef ENUM_CASE

        case SerdeDataType::StringOldVersion:
            return createStatisticsTypedJsonImpl<StatsType, String>(tag, blob);
        default: {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "unknown type");
        }
    }
}

StatisticsBasePtr createStatisticsBase(StatisticsTag tag, std::string_view blob)
{
    auto ptr = [&]() -> StatisticsBasePtr {
        switch (tag)
        {
#define UNTYPED_CASE(TYPE) \
    case StatisticsTag::TYPE: { \
        return createStatisticsUntyped<Stats##TYPE>(tag, blob); \
    }
            UNTYPED_STATS_ITERATE(UNTYPED_CASE)
#undef UNTYPED_CASE

#define TYPED_CASE(TYPE) \
    case StatisticsTag::TYPE: { \
        return createStatisticsTyped<Stats##TYPE>(tag, blob); \
    }
            TYPED_STATS_ITERATE(TYPED_CASE)
#undef TYPED_CASE

            default: {
                // unknonw statistics should be ignored instead of throw Exception
                return nullptr;
            }
        }
    }();

    return ptr;
}
StatisticsBasePtr createStatisticsBaseFromJson(StatisticsTag tag, std::string_view blob)
{
    auto ptr = [&]() -> StatisticsBasePtr {
        switch (tag)
        {
            case StatisticsTag::TableBasic:
                return createStatisticsUntypedJson<StatsTableBasic>(tag, blob);
            case StatisticsTag::ColumnBasic:
                return createStatisticsUntypedJson<StatsColumnBasic>(tag, blob);
            case StatisticsTag::NdvBucketsResult:
                return createStatisticsTypedJson<StatsNdvBucketsResult>(tag, blob);
            default: {
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unimplemented Statistics Tag");
            }
        }
    }();
    return ptr;
}
} // namespace DB
