#pragma once

#include <Query/Parsers/ASTHelper.h>
#include <Parsers/IAST_fwd.h>
#include <Analyzer/JoinNode.h>

#include <Query/Common/PredicateConst.h>

namespace DB
{
class ConstHashAST;

template <typename T>
using enable_if_ast = typename std::enable_if_t<std::is_same_v<T, ASTPtr> || std::is_same_v<T, ConstASTPtr>, bool>;
class PredicateUtils
{
public:

    static bool equals(ASTPtr & p1, ASTPtr & p2);
    static bool equals(ConstASTPtr & p1, ConstASTPtr & p2);
    static bool equals(ConstHashAST & p1, ConstHashAST & p2);

    /**
     * Extract predicate according 'and' function.
     *
     * (A & B & C & D) =>  A, B, C, D
     */
    template <typename T, enable_if_ast<T> = true>
    static std::vector<T> extractConjuncts(T predicate);

    /**
     * Extract predicate according 'or' function.
     *
     * (A & B) | (C & D) =>  (A & B), (C & D)
     */
    template <typename T, enable_if_ast<T> = true>
    static std::vector<T> extractDisjuncts(T predicate);

    /**
     * Extract predicate according function's name.
     */
    template <typename T, enable_if_ast<T> = true>
    static std::vector<T> extractPredicate(T predicate);

    /**
     * Extract sub-predicate :
     *
     * (A & B & C & D) | (E & F & G & H) | (I & J & K & L) =>
     *
     * A, B, C, D
     * E, F, G, H
     * I, J, K, L
     */
    static std::vector<std::vector<ConstASTPtr>> extractSubPredicates(ConstASTPtr predicate);

    /**
     * Extract common predicate
     *
     * (A & B & C & X & Y) | (E & F & G & X & Y) | (I & J & K & X & Y) =>
     *
     * (X & Y ) & ((A & B & C) | (E & F & G) | (I & J & K))
     */
    static ConstASTPtr extractCommonPredicates(ConstASTPtr predicate, ContextMutablePtr & context);

    /**
     * Applies the boolean distributive property.
     *
     * For example:
     * ( A & B ) | ( C & D ) => ( A | C ) & ( A | D ) & ( B | C ) & ( B | D)
     *
     * Returns the original expression if the expression is non-deterministic or if the distribution will
     * expand the expression by too much.
     */
    static ConstASTPtr distributePredicate(ConstASTPtr or_predicate, ContextMutablePtr & context);

    template <bool flatten = true, typename T, enable_if_ast<T> = true>
    static ASTPtr combineConjuncts(const absl::InlinedVector<T,7> & predicates);
    template <bool flatten = true, typename T, enable_if_ast<T> = true>
    static ASTPtr combineDisjuncts(const absl::InlinedVector<T,7> & predicates);
    template <bool flatten = true, typename T, enable_if_ast<T> = true>
    static ASTPtr combineDisjunctsWithDefault(const absl::InlinedVector<T,7> & predicates, const ASTPtr & default_ast);
    template <bool flatten = true, typename T, enable_if_ast<T> = true>
    static ASTPtr combinePredicates(const String & fun, absl::InlinedVector<T,7> predicates);

    template <typename T, enable_if_ast<T> = true>
    static bool isTruePredicate(const T & predicate);
    template <typename T, enable_if_ast<T> = true>
    static bool isFalsePredicate(const T & predicate);

    static bool containsAll(const Strings & partition_symbols, const std::set<String> & unique_symbols);
    static bool containsAny(const Strings & partition_symbols, const std::set<String> & unique_symbols);

    // static bool isInliningCandidate(ConstASTPtr & predicate, ProjectionNode & node);
    static ASTPtr extractJoinPredicate(JoinNode &);
    static bool isJoinClause(ConstASTPtr expression, std::set<String> & left_symbols, std::set<String> & right_symbols, ContextMutablePtr & context);
    static bool
    isJoinClauseUnmodified(std::set<std::pair<String, String>> & join_clauses, const Names & left_keys, const Names & right_keys);

    /**
     * @return residue expression if source expression is stronger than target,
     *         {@code true} if it is equal to target,
     *         {@code null} f it is weaker than target.
     */
    static ASTPtr splitPredicates(const ConstASTPtr & source, const ConstASTPtr & target);

    static std::pair<std::vector<std::pair<ConstASTPtr, ConstASTPtr>>, std::vector<ConstASTPtr>>
    extractEqualPredicates(const std::vector<ConstASTPtr> & predicates);

    // Set Operation
    static void subtract(ASTs & left, const ASTs & right);

private:
    static String flip(const String & fun_name);
    template <typename T, enable_if_ast<T> = true>
    static void extractPredicate(const T & predicate, const std::string & fun_name, std::vector<T> & result);
    static std::vector<std::pair<ConstASTPtr, String>>
    removeAll(std::vector<std::pair<ConstASTPtr, String>> & collection, std::set<String> & elements_to_remove);
    static std::vector<std::vector<ConstASTPtr>> cartesianProduct(std::vector<std::vector<ConstASTPtr>> &);
};

}
