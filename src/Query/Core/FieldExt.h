#pragma once

#include <Core/Field.h>

namespace DB
{

    [[nodiscard]] String toString(const Field & field)
    {
        switch (field.getType())
        {
            case Field::Types::UInt64:
                return std::to_string(field.get<UInt64>());
            case Field::Types::Int64:
                return std::to_string(field.get<Int64>());
            case Field::Types::Float64:
                return std::to_string(field.get<Float64>());
            case Field::Types::UInt128:
            {
                uint64_t high = field.get<UInt128>() << 64;
                uint64_t low = field.get<UInt128>() << 128;
                return fmt::format("{}{}", high, low);
            }
            case Field::Types::Int128:
            {
                int64_t high = field.get<Int128>() << 64;
                uint64_t low = field.get<UInt128>() << 128;
                return fmt::format("{}{}", high, low);
            }
            case Field::Types::String:
                return field.get<String>();

            default:
                // Other types are not currently supported
                return "";
        }
    }

}
