#pragma once

#include <Parsers/IAST.h>
#include <memory>
#include <unordered_map>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

namespace DB {

namespace ErrorCodes {
    extern const int UNKNOWN_TYPE_OF_AST_NODE;
}

/// Visitor pattern implementation to replace specific AST nodes with their extended versions.
class ASTReplaceVisitor {
public:
    static void replace(ASTPtr & node) {
        replaceInternal(node);
        if (auto ast_union_query = std::dynamic_pointer_cast<ASTSelectWithUnionQuery>(node))
            replaceInternal(ast_union_query->list_of_selects);
    }

private:
    using ReplacerFunc = ASTPtr (*)(const IAST &);

    static ASTPtr replacerExpressionList(const IAST & ast) {
        const auto * p_original = dynamic_cast<const ASTExpressionList*>(&ast);
        if (!p_original) {
            throw Exception(ErrorCodes::UNKNOWN_TYPE_OF_AST_NODE, "Expected ASTExpressionList node");
        }
        return std::make_shared<ASTExpressionList>(*p_original);
    }

    static ASTPtr replacerSelectQuery(const IAST & ast) {
        const auto * p_original = dynamic_cast<const ASTSelectQuery*>(&ast);
        if (!p_original) {
            throw Exception(ErrorCodes::UNKNOWN_TYPE_OF_AST_NODE, "Expected ASTSelectQuery node");
        }
        return std::make_shared<ASTSelectQuery>(*p_original);
    }
    
    inline static const std::unordered_map<std::string_view, ReplacerFunc> rule_map = {
        {"ExpressionList", &replacerExpressionList},
        {"SelectQuery",    &replacerSelectQuery}
    };

    static void replaceInternal(ASTPtr & node) {
        if (!node) return;

        const auto it = rule_map.find(node->getID());
        if (it != rule_map.end()) {
            if (auto new_node = it->second(*node)) {
                node = std::move(new_node);
            }
        }

        for (auto & child : node->children) {
            replaceInternal(child);
        }
    }
};

}
