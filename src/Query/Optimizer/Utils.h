#pragma once

#include <unordered_map>
#include <Query/Processors/QueryPlan/Assignment.h>
#include <Query/Processors/QueryPlan/PlanNode.h>
#include <Storages/IStorage_fwd.h>

#include <unordered_map>
#include <DataTypes/IDataType.h>

namespace DB
{
class ProjectionStep;
struct AggregateDescription;

namespace Utils
{
    using ConstASTPtr = std::shared_ptr<const IAST>;
    using NameToType = std::map<String, DataTypePtr>;

    void assertIff(bool expression1, bool expression2);
    void checkState(bool expression);
    void checkState(bool expression, const String & msg);
    void checkArgument(bool expression);
    void checkArgument(bool expression, const String & msg);
    bool isIdentity(const String & symbol, const ConstASTPtr & expression);
    bool isIdentity(const Assignment & assignment);
    bool isIdentity(const Assignments & assignments);
    bool isIdentity(const ProjectionStep & project);
    bool isAlias(const Assignment & assignment);
    bool isAlias(const Assignments & assignments);

    bool isIdentifierOrIdentifierCast(const ConstASTPtr & ast);
    // return inside expression if cast don't affect the data in the bound column, such as cast to Nullable(column_name), int8 to int32.
    ConstASTPtr tryUnwrapCast(const ConstASTPtr & expression, ContextMutablePtr context, const NamesAndTypes & names_and_types);

    NameToNameMap extractIdentities(const ProjectionStepExt & project);
    std::unordered_map<String, String> computeIdentityTranslations(const Assignments & assignments);
    ASTPtr extractAggregateToFunction(const AggregateDescription & agg_descr);
    bool containsAggregateFunction(const ASTPtr & ast);

    bool canIgnoreNullsDirection(const DataTypePtr & type);

template <typename T>
static std::vector<std::vector<T>> powerSet(std::vector<T> set)
{
    /*set_size of power set of a set with set_size
    n is (2**n -1)*/
    size_t pow_set_size = 1 << set.size();
    size_t counter, j;

    /*Run from counter 111..1 to 000..1 */
    std::vector<std::vector<T>> power_set;
    for (counter = pow_set_size - 1; counter > 0; counter--)
    {
        std::vector<T> subset;
        for (j = 0; j < set.size(); j++)
        {
            /* Check if jth bit in the counter is set
            If set then print jth element from set */
            if (counter & (1 << j))
                subset.emplace_back(set[j]);
        }
        power_set.emplace_back(subset);
    }
    return power_set;
}

    bool canChangeOutputRows(const Assignments & assignments, ContextPtr context);
    bool canChangeOutputRows(const ProjectionStepExt & project, ContextPtr context);

    // return nullopt if ambiguous symbol exists(rarely)
    std::optional<NameToType> extractNameToType(const PlanNodeBase & node);

    template <template <typename, typename...> typename Map, typename K, typename V>
    Map<V, K> reverseMap(const Map<K, V> & map)
    {
        Map<V, K> reversed;
        for (const auto & entry : map)
            reversed.emplace(entry.second, entry.first);
        return reversed;
    }

    std::string getVersionFromSystem();
}

}
