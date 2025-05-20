#include <Query/Planner/PlannerModels.h>

namespace DB
{

std::optional<String> FieldSymbolInfo::tryGetSubColumnSymbol(const SubColumnID & sub_column_id) const
{
    if (auto it = sub_column_symbols.find(sub_column_id); it != sub_column_symbols.end())
        return it->second;

    return std::nullopt;
}

const String & RelationPlan::getFirstPrimarySymbol() const
{
    if (field_symbol_infos.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No field symbols available in RelationPlan.");
    return field_symbol_infos.front().getPrimarySymbol();
}

}
