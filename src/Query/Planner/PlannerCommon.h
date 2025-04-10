#pragma once

#include <Query/Analyzer/ScopeAwareEquals.h>
#include <Core/Block.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Parsers/IAST_fwd.h>
#include <Query/Processors/QueryPlan/Assignment.h>
#include <Query/Planner/PlannerModels.h>

namespace DB
{

using NameToType = std::map<String, DataTypePtr>;

template <typename Container1, typename Container2>
void append(Container1 & v1, Container2 && v2)
{
    v1.reserve(v1.size() + v2.size());
    v1.insert(v1.end(), v2.begin(), v2.end());
}

template<typename T>
struct is_vector : public std::false_type {}; // NOLINT(readability-identifier-naming)

template<typename T, typename A>
struct is_vector<std::vector<T, A>> : public std::true_type {};

template <typename Container1, typename Container2, typename F>
void append(Container1 & v1, Container2 && v2, F && transformer)
{
    v1.reserve(v1.size() + v2.size());

    if constexpr(is_vector<Container1>::value){
        std::transform(v2.begin(), v2.end(), std::back_inserter(v1), transformer);
    }
    else {
        std::transform(v2.begin(), v2.end(), std::inserter(v1, v1.end()), transformer);
    }
}

template <typename Container, typename F>
Container deduplicateByAst(Container container, ScopePtr scope, Analysis & analysis, F && extractor)
{
    auto existing = createScopeAwaredASTSet(analysis, scope);
    container.erase(
        std::remove_if(container.begin(), container.end(), [&] (auto & elem) -> bool
                       {
                           return !existing.insert(extractor(elem)).second;
                       }),
        container.end());
    return container;
}

inline ASTPtr toSymbolRef(const String & symbol_name)
{
    return std::make_shared<ASTIdentifier>(symbol_name);
}

void putIdentities(const NamesAndTypes & columns, Assignments & assignments, NameToType & types);

inline void putIdentities(const Block & block, Assignments & assignments, NameToType & types)
{
    putIdentities(block.getNamesAndTypes(), assignments, types);
}

void mapFieldSymbolInfos(FieldSymbolInfos & symbol_infos, const NameToNameMap & name_mapping, bool map_sub_column);
}
