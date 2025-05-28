#pragma once

#include <memory>
#include <Core/Names.h>
#include <Functions/IFunction.h>
#include <Interpreters/WindowDescription.h>
#include <Parsers/IAST_fwd.h>
#include <Query/Processors/QueryPlan/QueryPlanStepHelper.h>

#include <string>
#include <unordered_map>

namespace DB
{

class PlanNodeStatisticsEstimate;
struct AggregatingTransformParamsExt;
using AggregatingTransformParamsExtPtr = std::shared_ptr<AggregatingTransformParamsExt>;

/**
 * Copy a step and replace its all symbol using mapping_function.
 */
class SymbolMapper
{
public:
    using Symbol = std::string;
    using MappingFunction = std::function<Symbol(const Symbol &)>;

    explicit SymbolMapper(MappingFunction mapping_function_) : mapping_function(std::move(mapping_function_)) { }

    /**
     * replace symbol using mapping
     * eg, if mapping: [a => a_1, a_1 => a_2], input: a, output: a_1
     */
    static SymbolMapper simpleMapper(std::unordered_map<Symbol, Symbol> & mapping);

    /**
     * replace symbol recursively using mapping
     * eg, if mapping: [a => a_1, a_1 => a_2], input: a, output: a_2
     */
    static SymbolMapper symbolMapper(std::unordered_map<Symbol, Symbol> & mapping);

    /**
     * replace symbol recursively using mapping.
     * if symbol is not record in mapping, create a new symbol using symbolAllocator.
     * eg, if mapping: [a => a_1, a_1 => a_2], input: b, output: b_2; input: a, output: a_2.
     */
    static SymbolMapper symbolReallocator(std::unordered_map<Symbol, Symbol> & mapping, SymbolAllocator & symbolAllocator);

    std::string map(const Symbol & symbol) {return mapping_function(symbol);}
    template <typename T>
    std::vector<T> map(const std::vector<T> & items)
    {
        std::vector<T> ret;
        std::transform(items.begin(), items.end(), std::back_inserter(ret), [&](const auto & param) { return SymbolMapper::map(param); });
        return ret;
    }

    static Names distinct(const Names & items)
    {
        Names ret;
        std::unordered_set<String> set;
        for (const auto & item : items)
            if (set.emplace(item).second)
                ret.emplace_back(item);
        return ret;
    }

    NameSet mapToDistinct(const Names & symbols);
    NamesAndTypes map(const NamesAndTypes & name_and_types);
    NameSet map(const NameSet & names);

    NameToType map(const NameToType & name_to_type);
    NamesWithAliases map(const NamesWithAliases & name_with_aliases);
    Assignments map(const Assignments & assignments);
    Assignment map(const Assignment & assignment);
    Block map(const Block & name_and_types);
    DataStream map(const DataStream & data_stream);
    ASTPtr map(const ASTPtr & expr);
    ASTPtr map(const ConstASTPtr & expr);
    Partitioning map(const Partitioning & partition);
    AggregateDescription map(const AggregateDescription & desc);
    GroupingSetsParamsExt map(const GroupingSetsParamsExt & param);
    WindowFunctionDescription map(const WindowFunctionDescription & desc);
    WindowDescription map(const WindowDescription & desc);
    SortColumnDescription map(const SortColumnDescription & desc);
    AggregatorExt::Params map(const AggregatorExt::Params & params);
    AggregatingTransformParamsExtPtr map(const AggregatingTransformParamsExtPtr & param);
    ArrayJoinActionPtr map(const ArrayJoinActionPtr & array_join_action);
    GroupingDescription map(const GroupingDescription & desc);
    SortDescription map(const SortDescription & sort_desc);
    std::map<Int32, Names> map(const std::map<Int32, Names> & group_id_non_null_symbol);
    SortColumnDescriptionWithColumnIndex map(const SortColumnDescriptionWithColumnIndex & sort_column_description);

    LinkedHashMap<String, RuntimeFilter> map(const LinkedHashMap<String, RuntimeFilter> & infos);
    PlanNodeStatisticsEstimate map(const PlanNodeStatisticsEstimate & estimate);

#define VISITOR_DEF(TYPE) std::shared_ptr<TYPE> map(const TYPE &);
    APPLY_PROTOBUF_STEP_TYPES(VISITOR_DEF)
#undef VISITOR_DEF

    QueryPlanStepPtr map(const IQueryPlanStep & step);

private:
    MappingFunction mapping_function;
    class IdentifierRewriter;
    class SymbolMapperVisitor;
};

}
