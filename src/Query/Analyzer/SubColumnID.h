#pragma once

#include <Core/Types.h>

namespace DB
{

struct SubColumnID
{
    enum class Type
    {
        /// sub-columns for each map element of a Map column, e.g. map_col{'a'} ==> __map_col__a
        MAP_ELEMENT,

        /// sub-column storing all map keys of a Map column, e.g. mapKeys(map_col) ==> map_col.key
        MAP_KEYS,

        /// sub-column storing all map values of a Map column, e.g. mapValues(map_col) ==> map_col.value
        MAP_VALUES,

        JSON_FIELD,
    };

    Type type;
    String map_element_key;
    String json_field_name;

    String getSubColumnName(const String &) const;

    bool operator==(const SubColumnID & other) const;

    struct Hash
    {
        size_t operator()(const SubColumnID & id) const;
    };

    static inline SubColumnID mapElement(const String & map_element_key)
    {
        SubColumnID id;
        id.type = Type::MAP_ELEMENT;
        id.map_element_key = map_element_key;
        return id;
    }

    static inline SubColumnID mapKeys()
    {
        SubColumnID id;
        id.type = Type::MAP_KEYS;
        return id;
    }

    static inline SubColumnID mapValues()
    {
        SubColumnID id;
        id.type = Type::MAP_VALUES;
        return id;
    }

    static inline SubColumnID jsonField(const String & json_field_name)
    {
        SubColumnID id;
        id.type = Type::JSON_FIELD;
        id.json_field_name = json_field_name;
        return id;
    }
};

}
