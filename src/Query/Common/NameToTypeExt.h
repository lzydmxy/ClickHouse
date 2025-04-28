#pragma once

#include <Core/ColumnsWithTypeAndName.h>


namespace DB
{
using NameToType = std::map<String, DataTypePtr>;
using NamesAndTypes = std::vector<NameAndTypePair>;

NameToType ToNameToType(const ColumnsWithTypeAndName & data)
{
    NameToType res;
    for (const auto & column : data)
        res.emplace(column.name, column.type);

    return res;
}

ColumnsWithTypeAndName ToColumnsWithTypeAndName(const NameToType & data)
{
    ColumnsWithTypeAndName res;
    for (const auto & [name, type] : data)
        res.emplace_back(type, name);

    return res;
}

NamesAndTypes ToNamesAndTypes(const ColumnsWithTypeAndName & data)
{
    NamesAndTypes res;
    for (const auto & column : data)
        res.emplace_back(column.name, column.type);

    return res;
}

ColumnsWithTypeAndName ToColumnsWithTypeAndName(const NamesAndTypes & data)
{
    ColumnsWithTypeAndName res;
    for (const auto & [name, type] : data)
        res.emplace_back(type, name);

    return res;
}

NameSet ToNameSet(const NamesAndTypes & data)
{
    NameSet res;
    res.reserve(data.size());

    for (const auto & elem : data)
        res.insert(elem.name);

    return res;
}

}
