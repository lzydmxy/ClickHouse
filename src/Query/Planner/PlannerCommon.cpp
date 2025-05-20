#include <Query/Planner/PlannerCommon.h>

namespace DB
{

void putIdentities(const NamesAndTypes & columns, Assignments & assignments, NameToType & types)
{
    for (const auto & col: columns)
    {
        assignments.emplace_back(col.name, toSymbolRef(col.name));
        types[col.name] = col.type;
    }
}

void mapFieldSymbolInfos(FieldSymbolInfos & symbol_infos, const NameToNameMap & name_mapping, bool map_sub_column)
{
    for (auto & symbol_info : symbol_infos)
    {
        if (auto it = name_mapping.find(symbol_info.primary_symbol); it != name_mapping.end())
            symbol_info.primary_symbol = it->second;

        if (map_sub_column)
        {
            for (auto & sub_symbol_info : symbol_info.sub_column_symbols)
                if (auto it = name_mapping.find(sub_symbol_info.second); it != name_mapping.end())
                    sub_symbol_info.second = it->second;
        }
    }
}

}
