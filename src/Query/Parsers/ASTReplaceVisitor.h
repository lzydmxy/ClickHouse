#pragma once

#include <Parsers/IAST.h>
#include <array>
#include <memory>
#include <unordered_map>
#include <Query/Parsers/ASTSelectQueryExt.h>
#include <Query/Parsers/ASTExpressionListExt.h>

namespace DB {

class ASTReplaceVisitor {
public:
    static void replace(ASTPtr & node) {
        replaceInternal(node);
    }

private:
    using ReplacerFunc = ASTPtr (*)(const IAST &);
    
    static constexpr std::array<std::pair<const char*, ReplacerFunc>, 2> rules = {{
        {typeid(ASTExpressionList).name(), &replacerExpressionList},
        {typeid(ASTSelectQuery).name(),     &replacerSelectQuery}
    }};

    static ASTPtr replacerExpressionList([[maybe_unused]] const IAST & ast) {
        return std::make_shared<ASTExpressionListExt>();
    }

    static ASTPtr replacerSelectQuery([[maybe_unused]] const IAST & ast) {
        return std::make_shared<ASTSelectQueryExt>();
    }

    static void replaceInternal(ASTPtr & node) {
        if (!node) return;

        static const auto rule_map = []{
            std::unordered_map<std::string_view, ReplacerFunc> map;
            for (const auto& [id, func] : rules) {
                map.emplace(id, func);
            }
            return map;
        }();

        const auto it = rule_map.find(typeid(*node).name());
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

} // namespace DB
