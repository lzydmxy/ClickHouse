#pragma once

#include <Parsers/IAST.h>
#include <memory>
#include <unordered_map>
#include <Query/Parsers/ASTSelectQueryExt.h>
#include <Query/Parsers/ASTExpressionListExt.h>

namespace DB {

namespace ErrorCodes {
    extern const int AST_TYPE_MISMATCH;
}

/// Visitor pattern implementation to replace specific AST nodes with their extended versions.
class ASTReplaceVisitor {
public:
    static void replace(ASTPtr & node) {
        replaceInternal(node);
    }

private:
    using ReplacerFunc = ASTPtr (*)(const IAST &);

    inline static const std::unordered_map<std::string_view, ReplacerFunc> rule_map = {
        {"ExpressionList", &replacerExpressionList},
        {"SelectQuery",    &replacerSelectQuery}
    };

    static ASTPtr replacerExpressionList(const IAST & ast) {
        const auto * p_original = dynamic_cast<const ASTExpressionList*>(&ast);
        if (!p_original) {
            throw Exception(ErrorCodes::AST_TYPE_MISMATCH, "Expected ASTExpressionList node");
        }
        return std::make_shared<ASTExpressionListExt>(*p_original);
    }

    static ASTPtr replacerSelectQuery(const IAST & ast) {
        const auto * p_original = dynamic_cast<const ASTSelectQuery*>(&ast);
        if (!p_original) {
            throw Exception(ErrorCodes::AST_TYPE_MISMATCH, "Expected ASTSelectQuery node");
        }
        return std::make_shared<ASTSelectQueryExt>(*p_original);
    }
    
    static void replaceInternal(ASTPtr & node) {
        if (!node) return;

        const auto it = rule_map.find(node->getID());
        if (it != rule_map.end()) {
            if (auto new_node = it->second(*node)) {
                node = std::move(new_node);
                replaceInternal(node);
                return;
            }
        }

        for (auto & child : node->children) {
            replaceInternal(child);
        }
    }
};

}
