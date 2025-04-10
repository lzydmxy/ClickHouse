#pragma once
#include <string>
#include <Query/Protos/EnumMacros.h>
#include <Query/Protos/common.pb.h>

namespace DB
{

class ASOFJoinInequalityConverter
{
public:
    using _enumType = ASOFJoinInequality;

    [[maybe_unused]] static Protos::ASOFJoinInequality::Enum toProto(const _enumType & _value)
    {
        switch (_value)
        {
            case _enumType::None:
                return Protos::ASOFJoinInequality::None;
            case _enumType::Less:
                return Protos::ASOFJoinInequality::Less;
            case _enumType::Greater:
                return Protos::ASOFJoinInequality::Greater;
            case _enumType::LessOrEquals:
                return Protos::ASOFJoinInequality::LessOrEquals;
            case _enumType::GreaterOrEquals:
                return Protos::ASOFJoinInequality::GreaterOrEquals;
        }
    }

    [[maybe_unused]] static _enumType fromProto(const Protos::ASOFJoinInequality::Enum & proto)
    {
        switch (proto)
        {
            case Protos::ASOFJoinInequality::None:
                return _enumType::None;
            case Protos::ASOFJoinInequality::Less:
                return _enumType::Less;
            case Protos::ASOFJoinInequality::Greater:
                return _enumType::Greater;
            case Protos::ASOFJoinInequality::LessOrEquals:
                return _enumType::LessOrEquals;
            case Protos::ASOFJoinInequality::GreaterOrEquals:
                return _enumType::GreaterOrEquals;
            default: { throwBetterEnumException("protobuf", "Inequality", static_cast<int>(proto)); }
        }
    }

    [[maybe_unused]] static const std::string & toString(const _enumType & _value)
    {
        return Protos::ASOFJoinInequality::Enum_Name(toProto(_value));
    }
};

}
