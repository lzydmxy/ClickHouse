#pragma once

#include <Interpreters/Context.h>
#include <Query/Parsers/ASTVisitor.h>
#include <Parsers/IAST.h>
#include <Query/Processors/QueryPlan/Assignment.h>

namespace DB
{
    class ExpressionDeterminism
    {
    public:
        static std::set<String> getDeterministicSymbols(const Assignments & assignments, ContextPtr context);
        static ConstASTPtr filterDeterministicConjuncts(ConstASTPtr predicate, ContextPtr context);
        static ConstASTPtr filterNonDeterministicConjuncts(ConstASTPtr predicate, ContextPtr context);
        static std::set<ConstASTPtr> filterDeterministicPredicates(ConstASTs & predicates, ContextPtr context);
        static bool isDeterministic(ConstASTPtr expression, ContextPtr context);
        static bool canChangeOutputRows(ConstASTPtr expression, ContextPtr context);

        struct ExpressionProperty
        {
            bool is_deterministic;
            bool can_change_output_rows;
        };

    private:
        static ExpressionProperty getExpressionProperty(ConstASTPtr expression, ContextPtr context);
    };

    class DeterminismVisitor : public ConstASTVisitor<Void, ContextPtr>
    {
    public:
        explicit DeterminismVisitor(bool isDeterministic);
        Void visitNode(const ConstASTPtr & node, ContextPtr & context) override;
        Void visitASTFunction(const ConstASTPtr & node, ContextPtr & context) override;
        bool isDeterministic() const { return is_deterministic; }
        bool canChangeOutputRows() const { return can_change_output_rows; }

    private:
        bool is_deterministic;
        bool can_change_output_rows = false;
    };

}
