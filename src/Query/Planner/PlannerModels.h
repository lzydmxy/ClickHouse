#pragma once

#include <Core/Types.h>
#include <Query/Analyzer/Analysis.h>
#include <Query/Analyzer/SubColumnID.h>
#include <Query/Processors/QueryPlan/PlanNode.h>

namespace DB
{

struct FieldSymbolInfo
{
    using SubColumnToSymbol = std::unordered_map<SubColumnID, String, SubColumnID::Hash>;

    String primary_symbol;
    SubColumnToSymbol sub_column_symbols;

    FieldSymbolInfo(String primary_symbol_ = ""): primary_symbol(std::move(primary_symbol_)) // NOLINT(google-explicit-constructor)
    {}

    FieldSymbolInfo(String primary_symbol_, SubColumnToSymbol sub_column_symbols_)
        : primary_symbol(std::move(primary_symbol_)), sub_column_symbols(std::move(sub_column_symbols_))
    {}

    const String & getPrimarySymbol() const
    {
        return primary_symbol;
    }

    std::optional<String> tryGetSubColumnSymbol(const SubColumnID & sub_column_id) const;
};

using FieldSymbolInfos = std::vector<FieldSymbolInfo>;

struct RelationPlan
{
    PlanNodePtr root;
    FieldSymbolInfos field_symbol_infos;

    RelationPlan() = default;
    RelationPlan(PlanNodePtr root_, FieldSymbolInfos field_symbol_infos_)
        : root(std::move(root_)), field_symbol_infos(std::move(field_symbol_infos_))
    {
    }

    void withNewRoot(PlanNodePtr new_root) { root = std::move(new_root); }
    PlanNodePtr getRoot() const { return root; }
    const FieldSymbolInfos & getFieldSymbolInfos() const { return field_symbol_infos; }
    const String & getFirstPrimarySymbol() const;
};

using RelationPlans = std::vector<RelationPlan>;

using CTERelationPlans = std::unordered_map<CTEId, RelationPlan>;

struct PlanWithSymbolMappings
{
    PlanNodePtr plan;
    NameToNameMap mappings;
};

struct PlanWithSymbols
{
    PlanNodePtr plan;
    Names symbols;
};

}
