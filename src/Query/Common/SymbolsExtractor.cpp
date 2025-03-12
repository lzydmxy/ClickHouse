#include <Query/Common/SymbolsExtractor.h>

#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

std::vector<std::string> SymbolsExtractor::extractVector(ConstASTPtr node)
{
    if (!node)
        return {};

    SymbolVisitor visitor;
    SymbolVisitorContext context;
    ASTVisitorUtil::accept(node, visitor, context);
    if (!context.exclude_symbols.empty())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "exclude_symbols should be null");
    }

    return std::move(context.result);
}

std::set<std::string> SymbolsExtractor::extract(ConstASTPtr node)
{
    auto result = extractVector(node);
    return std::set(result.begin(), result.end());
}

// TODO need ExpressionExtractor
//std::set<std::string> SymbolsExtractor::extract(PlanNodePtr & node)
//{
//    std::vector<ConstASTPtr> expressions;
//    for (ConstASTPtr expr : ExpressionExtractor::extract(node))
//    {
//        expressions.emplace_back(std::move(expr));
//    }
//    return extract(expressions);
//}

std::set<std::string> SymbolsExtractor::extract(std::vector<ConstASTPtr> & nodes)
{
    SymbolVisitor visitor;
    SymbolVisitorContext context;
    for (auto & node : nodes)
    {
        if (node)
            ASTVisitorUtil::accept(node, visitor, context);
    }
    return std::set(context.result.begin(), context.result.end());
}

Void SymbolVisitor::visitNode(const ConstASTPtr & node, SymbolVisitorContext & context)
{
    for (ConstASTPtr child : node->children)
    {
        ASTVisitorUtil::accept(child, *this, context);
    }
    return Void{};
}

Void SymbolVisitor::visitASTIdentifier(const ConstASTPtr & node, SymbolVisitorContext & context)
{
    const auto & identifier = node->as<ASTIdentifier &>();
    if (context.exclude_symbols.empty() || !context.exclude_symbols.count(identifier.name()))
    {
        context.result.emplace_back(identifier.name());
    }
    return Void{};
}

Void SymbolVisitor::visitASTFunction(const ConstASTPtr & node, SymbolVisitorContext & context)
{
    const auto & ast_func = node->as<const ASTFunction &>();
    if (unlikely(ast_func.name == "lambda"))
    {
        auto exclude_symbols = RequiredSourceColumnsMatcher::extractNamesFromLambda(ast_func);
        for (auto & es : exclude_symbols)
        {
            ++context.exclude_symbols[es];
        }

        visitNode(ast_func.arguments->children[1], context);

        for (auto & es : exclude_symbols)
        {
            auto reduced_value = --context.exclude_symbols[es];
            if (reduced_value == 0)
            {
                context.exclude_symbols.erase(es);
            }
        }

        return Void{};
    }
    else
    {
        return visitNode(node, context);
    }
}

}
