#pragma once

#include <Core/SortDescription.h>
#include <Query/Optimizer/Property/Constants.h>
#include <Query/Optimizer/Property/Property.h>
#include <Query/Optimizer/Property/SymbolEquivalencesDeriver.h>

namespace DB
{
class PropertyMatcher
{
public:
    static bool matchNodePartitioning(
        const Context & context, Partitioning & required, const Partitioning & actual, const SymbolEquivalences & equivalences = {}, const Constants & constants = {});

    static bool matchStreamPartitioning(
        const Context & context, const Partitioning & required, const Partitioning & actual, const SymbolEquivalences & equivalences = {}, const Constants & constants = {}, bool match_local_exchange = true);

    static Sorting
    matchSorting(const Context & context, const Sorting & required, const Sorting & actual, const SymbolEquivalences & equivalences = {}, const Constants & constants = {});

    static Sorting matchSorting(
        const Context & context, const SortDescription & required, const Sorting & actual, const SymbolEquivalences & equivalences = {}, const Constants & constants = {});

    static Property compatibleCommonRequiredProperty(const std::unordered_set<Property, PropertyHash> & properties);
};
}
