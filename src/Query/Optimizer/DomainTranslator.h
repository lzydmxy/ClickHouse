#pragma once

#include <Analyzers/ASTEquals.h>
#include <Analyzers/TypeAnalyzer.h>
#include <Columns/ColumnSet.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Query/Optimizer/ExpressionDeterminism.h>
#include <Query/Optimizer/ExpressionInterpreter.h>
#include <Query/Optimizer/FunctionInvoker.h>
#include <Query/Optimizer/LiteralEncoder.h>
#include <Query/Optimizer/PredicateUtils.h>
#include <Query/Optimizer/Utils.h>
#include <Query/Optimizer/domain.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTVisitor.h>
#include <Parsers/IAST_fwd.h>
#include <Common/UTF8Helpers.h>

#include <optional>
#include <type_traits>
#include <utility>
#include <assert.h>

namespace DB::Predicate
{

struct NormalizedSimpleComparison
{
    ASTPtr symbol_expression;
    String operator_name;
    FieldWithType value_with_type;
    NormalizedSimpleComparison(ASTPtr & symbol_expression_, String & operator_name_, FieldWithType & value_with_type_) :
        symbol_expression(symbol_expression_), operator_name(operator_name_), value_with_type(value_with_type_){}
};

template <typename T>
struct ExtractionResult
{
    TupleDomain<T> tuple_domain;
    ASTPtr remaining_expression;
    ExtractionResult(TupleDomain<T> tuple_domain_, ASTPtr remaining_expression_)
        : tuple_domain(std::move(tuple_domain_)), remaining_expression(std::move(remaining_expression_))
    {
    }
};

//TODO: ConstASTVisitor
template <typename T>
class DomainVisitor : public ASTVisitor<ExtractionResult<T>, const bool>
{
public:
    DomainVisitor(ContextMutablePtr context_, TypeAnalyzer & type_analyzer_, NameToType column_types_, bool & is_ignored_) : context(context_), type_analyzer(type_analyzer_), column_types(std::move(column_types_)), is_ignored(is_ignored_){}
    ExtractionResult<T> process(ASTPtr & node, const bool & complement);
    ExtractionResult<T> visitASTFunction(ASTPtr & node, const bool & complement) override;
    ExtractionResult<T> visitNode(ASTPtr & node, const bool & complement) override;
    ExtractionResult<T> visitASTLiteral(ASTPtr & node, const bool & complement) override;
    ExtractionResult<T> visitASTIdentifier(ASTPtr & node, const bool & complement) override;
    ExtractionResult<T> visitLogicalFunction(ASTPtr & node, const bool & complement, const String & fun_name);
    ExtractionResult<T> visitNotFunction(ASTPtr & node, const bool & complement);
    ExtractionResult<T> visitComparisonFunction(ASTPtr & node, const bool & complement);
    ExtractionResult<T> visitInFunction(ASTPtr & node, const bool & complement);
    ExtractionResult<T> visitLikeFunction(ASTPtr & node, const bool & complement);
    ExtractionResult<T> visitStartsWithFunction(ASTPtr & node, const bool & complement);
    ExtractionResult<T> visitIsNullFunction(ASTPtr & node, const bool & complement);
    ExtractionResult<T> visitIsNotNullFunction(ASTPtr & node, const bool & complement);

private:
    ContextMutablePtr context;
    TypeAnalyzer & type_analyzer;
    NameToType column_types;
    bool & is_ignored;
    DataTypePtr checkedTypeLookup(const T & symbol) const;
    ASTPtr complementIfNecessary(const ASTPtr & ast, bool complement) const;
    std::vector<TupleDomain<T>> extractTupleDomains(const std::vector<ExtractionResult<T>> & results) const;
    ConstASTs extractRemainingExpressions(const std::vector<ExtractionResult<T>> & results) const;

    std::optional<NormalizedSimpleComparison> toNormalizedSimpleComparison(ASTPtr & comparison) const;
    std::optional<ExtractionResult<T>> processSimpleInPredicate(ASTPtr & node, const bool & complement);
    std::optional<Domain> createRangeDomain(const DataTypePtr & type, const String & constant_prefix);

    std::optional<ExtractionResult<T>> createComparisonExtractionResult(
        ASTPtr & node,
        const String & operator_name,
        const T & column,
        const DataTypePtr & type,
        const Field & field,
        const bool & complement);
    static std::optional<Domain> extractOrderableDomain(const String & operator_name, const DataTypePtr & type, const Field & value, const bool & complement);
    static Domain extractDiscreteDomain(const String & operator_name, const DataTypePtr & type, const Field & value, const bool & complement);
    static bool isFloatingPointNaN(const DataTypePtr & type, const Field & value);
    static bool isValidOperatorForComparison(const String & operator_name);

    bool allTupleDomainsAreSameSingleColumn(const std::vector<TupleDomain<T>> & tuple_domains) const;
    bool allTupleDomainsAreNotAll(const std::vector<TupleDomain<T>> & tuple_domains) const;

    std::optional<Field> canImplicitCoerceValue(Field & value, DataTypePtr & from_type, DataTypePtr & to_type) const;
    std::optional<Field> getConvertFieldToType(Field & value, DataTypePtr & from_type, DataTypePtr & to_type) const;

    std::optional<T> checkAndGetSymbol(const ASTPtr & ast) const
    {
        if constexpr (std::is_same_v<T, String>)
        {
            if (const auto * iden = ast->as<ASTIdentifier>())
                return iden->name();

            return std::nullopt;
        }
        else
        {
            if (ExpressionDeterminism::isDeterministic(ast, context))
                return ast;

            return std::nullopt;
        }
    }
};

template <typename T>
class DomainTranslator
{
public:
    DomainTranslator(ContextMutablePtr context_): context(context_), is_ignored(false) {}
    ASTPtr toPredicate(const TupleDomain<T> & tuple_domain);
    ASTPtr toPredicate(const ASTPtr & symbol, const Domain & domain);
    ExtractionResult<T> getExtractionResult(ASTPtr predicate, NamesAndTypes types);
    ExtractionResult<T> getExtractionResult(ASTPtr predicate, NameToType types);
    bool isIgnored() { return is_ignored; }
private:
    ContextMutablePtr context;
    bool is_ignored;
    ConstASTs extractDisjuncts(const DataTypePtr & type, const Ranges & ranges, ASTPtr symbol);
    ConstASTs extractDisjuncts(const DataTypePtr & type, const DiscreteValueSet & discrete_value_set, ASTPtr symbol);
    ASTPtr processRange(const DataTypePtr & type, const Range & range, ASTPtr & symbol);
    ASTPtr combineRangeWithExcludedPoints(const DataTypePtr & type, ASTPtr & symbol, const Range & range, ASTs & excluded_points);
    ASTPtr literalEncodeWithType(const DataTypePtr & type, const Field & field);
    Ranges pickOutSingleValueRanges(const SortedRangeSet & sorted_range_set);
    static bool anyRangeIsAll(const Ranges & ranges);

};

size_t patternConstantPrefixBytes(const String & pattern);
String extractFixedStringFromLikePattern(const String & like_pattern);
}
