#pragma once

#include <Query/Analyzer/ExprAnalyzer.h>

#include <Core/NamesAndTypes.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <boost/core/noncopyable.hpp>

namespace DB
{

class TypeAnalyzer;
using TypeAnalyzerPtr = std::shared_ptr<TypeAnalyzer>;
using ExpressionTypes = std::unordered_map<ASTPtr, DataTypePtr>;
using NameToType = std::map<String, DataTypePtr>;

/**
 * AST type analyzer.
 *
 * Analyze and return the type of given expression.
 */
class TypeAnalyzer
{
public:
    // WARNING: this can be slow
    // if you will use the same `input_types` to getType for many times
    // use the following instead
    // ```
    // auto analyzer = TypeAnalyzer::create(context, input_types);
    // for (...) {...; analyzer.getType(expr); ...;}
    // ```
    static DataTypePtr getType(const ConstASTPtr & expr, ContextPtr context, const NamesAndTypes & input_types);

    static TypeAnalyzer create(ContextPtr context, const NameToType & input_types);
    static TypeAnalyzer create(ContextPtr context, const NamesAndTypes & input_types);
    DataTypePtr getType(const ConstASTPtr & expr) const;
    DataTypePtr getTypeWithoutCheck(const ConstASTPtr & expr) const;
    ExpressionTypes getExpressionTypes(const ConstASTPtr & expr) const;

    TypeAnalyzer(TypeAnalyzer && other) = default;

private:
    TypeAnalyzer(ContextPtr context_, Scope && scope_) : context(std::move(context_)), scope(std::move(scope_))
    {
    }

    ContextPtr context;
    Scope scope;
};


}
