#pragma once

#include <Query/Parsers/ASTHelper.h>
#include <Query/Parsers/ASTVisitor.h>
#include <Query/Processors/QueryPlan/PlanVisitor.h>

namespace DB
{
class SymbolsExtractor
{
public:
    static std::vector<std::string> extractVector(ConstASTPtr node);
    static std::set<std::string> extract(ConstASTPtr node);
    // TODO need ExpressionExtractor
    // static std::set<std::string> extract(PlanNodePtr & node);
    static std::set<std::string> extract(std::vector<ConstASTPtr> & nodes);
};


struct SymbolVisitorContext
{
    std::vector<std::string> result;
    // count of forbidden list
    std::unordered_map<std::string, UInt64> exclude_symbols;
};


class Void
{
};

class SymbolVisitor : public ConstASTVisitor<Void, SymbolVisitorContext>
{
public:
    Void visitNode(const ConstASTPtr &, SymbolVisitorContext & context) override;
    Void visitASTIdentifier(const ConstASTPtr &, SymbolVisitorContext & context) override;
    Void visitASTFunction(const ConstASTPtr & node, SymbolVisitorContext & context) override;
};

}
