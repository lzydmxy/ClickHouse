#pragma once

#include <Parsers/IAST.h>

namespace DB
{

struct ASTReplaceRuler {
    String target_id;          // 需要替换的节点类型ID（如 "ASTSelectQuery"）
    std::function<ASTPtr(const IAST &)> replacer;  // 替换函数
};


class ASTReplaceVisitor {
    public:
        // 静态方法执行替换
        static void replace(ASTPtr & node, const ASTReplaceRuler & rule, const ASTPtr & parent = nullptr) {
            if (!node) return;
    
            // 检查是否匹配当前节点
            if (node->getID() == rule.target_id) {
                // 执行替换
                ASTPtr new_node = rule.replacer(*node);
                if (new_node) {
                    node = std::move(new_node);
                }
            }
    
            // 递归处理子节点
            for (auto & child : node->children) {
                replace(child, rule, node);
            }
        }
    
        // 支持多规则替换的重载方法
        static void replace(ASTPtr & node, const std::vector<ASTReplaceRuler> & rules, const ASTPtr & parent = nullptr) {
            if (!node) return;
    
            // 检查所有规则
            for (const auto & rule : rules) {
                if (node->getID() == rule.target_id) {
                    ASTPtr new_node = rule.replacer(*node);
                    if (new_node) {
                        node = std::move(new_node);
                        break;  // 替换后不再检查其他规则
                    }
                }
            }
    
            // 递归处理子节点
            for (auto & child : node->children) {
                replace(child, rules, node);
            }
        }
};


    // 替换规则1：ASTSelectQuery → ASTSelectQueryExt
// ASTReplaceRuler replaceSelectRule{
//     "ASTSelectQuery",
//     [](const IAST & node) {
//         const auto & orig = dynamic_cast<const ASTSelectQuery &>(node);
//         auto new_node = std::make_shared<ASTSelectQueryExt>();
//         new_node->children = orig.children;
//         new_node->select = orig.select;
//         new_node->from = orig.from;
//         new_node->where = orig.where;
//         return new_node;
//     }
// };

// 替换规则2：ASTExpressionList → ASTExpressionListExt
// ASTReplaceRuler replaceExprListRule{
//     "ASTExpressionList",
//     [](const IAST & node) {
//         const auto & orig = dynamic_cast<const ASTExpressionList &>(node);
//         auto new_node = std::make_shared<ASTExpressionListExt>();
//         new_node->children = orig.children;
//         new_node->separator = orig.separator;
//         return new_node;
//     }
// };

//TODO: ASTVisitor
 
// 多规则替换示例
// std::vector<ASTReplaceRuler> rules = {replaceSelectRule, replaceExprListRule};
// ASTReplaceVisitor::replace(root_node, rules);
}
