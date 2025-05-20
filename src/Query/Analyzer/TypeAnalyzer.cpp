#include <Query/Analyzer/TypeAnalyzer.h>

#include <Query/Analyzer/ExprAnalyzer.h>
#include <Query/Analyzer/Analysis.h>


namespace DB
{

DataTypePtr TypeAnalyzer::getType(const ConstASTPtr & expr, ContextPtr context, const NamesAndTypes & input_types)
{
    return TypeAnalyzer::create(context, input_types).getType(expr);
}

TypeAnalyzer TypeAnalyzer::create(ContextPtr context, const NameToType & input_types)
{
    NamesAndTypes names_and_types;

    for (const auto & [name, type]: input_types)
        names_and_types.emplace_back(name, type);

    return create(context, names_and_types);
}

TypeAnalyzer TypeAnalyzer::create(ContextPtr context, const NamesAndTypes & input_types)
{
    FieldDescriptions fields;
    for(const auto & input : input_types) {
        FieldDescription field {input.name, input.type};
        fields.emplace_back(field);
    }
    Scope scope(Scope::ScopeType::RELATION, nullptr, true, fields);
    return TypeAnalyzer(context, std::move(scope));
}

#define REMOVE_CONST(const_ptr) (std::const_pointer_cast<IAST>(const_ptr))

DataTypePtr TypeAnalyzer::getType(const ConstASTPtr & expr) const
{
    Analysis analysis;
    ExprAnalyzerOptions options;
    options.expandUntuple(false);
    ASTPtr tmp_ast = REMOVE_CONST(expr);
    return ExprAnalyzer::analyze(tmp_ast, &scope, context, analysis, options);
}

DataTypePtr TypeAnalyzer::getTypeWithoutCheck(const ConstASTPtr & expr) const
{
    Analysis analysis;
    ExprAnalyzerOptions options;
    options.expandUntuple(false);
    options.aggregateSupport(ExprAnalyzerOptions::AggregateSupport::ALLOWED);
    options.windowSupport(ExprAnalyzerOptions::WindowSupport::ALLOWED);
    ASTPtr tmp_ast = REMOVE_CONST(expr);
    return ExprAnalyzer::analyze(tmp_ast, &scope, context, analysis, options);
}

ExpressionTypes TypeAnalyzer::getExpressionTypes(const ConstASTPtr & expr) const
{
    Analysis analysis;
    ExprAnalyzerOptions options;
    options.expandUntuple(false);
    ASTPtr tmp_ast = REMOVE_CONST(expr);
    ExprAnalyzer::analyze(tmp_ast, &scope, context, analysis, options);
    return analysis.getExpressionTypes();
}

}
