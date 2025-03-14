#pragma once

#include <Query/Parsers/ASTHelper.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_DEEP_RECURSION;
}

template <typename R, typename C>
class ASTVisitor
{
public:
    constexpr static UInt64 MAX_RECURSION_LEVEL = 1024;

    explicit ASTVisitor(UInt64 max_level_ = MAX_RECURSION_LEVEL) : max_level(max_level_)
    {
    }
    virtual ~ASTVisitor() = default;
    virtual R visitNode(ASTPtr &, C &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Visitor does not supported this AST node."); }
#define VISITOR_DEF(TYPE) \
    virtual R visit##TYPE(ASTPtr & node, C & context) { return visitNode(node, context); }
    APPLY_AST_TYPES(VISITOR_DEF)
#undef VISITOR_DEF

private:
    UInt64 max_level;
    UInt64 level = 0;
    friend class ASTVisitorUtil;
};


template <typename R, typename C>
class ConstASTVisitor
{
public:
    constexpr static UInt64 MAX_RECURSION_LEVEL = 1024;

    explicit ConstASTVisitor(UInt64 max_level_ = MAX_RECURSION_LEVEL) : max_level(max_level_)
    {
    }
    virtual ~ConstASTVisitor() = default;
    virtual R visitNode(const ConstASTPtr &, C &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Visitor does not supported this AST node."); }
#define VISITOR_DEF(TYPE) \
    virtual R visit##TYPE(const ConstASTPtr & node, C & context) { return visitNode(node, context); }
    APPLY_AST_TYPES(VISITOR_DEF)
#undef VISITOR_DEF

private:
    UInt64 max_level;
    UInt64 level = 0;
    friend class ASTVisitorUtil;
};

class ASTVisitorUtil
{
public:
    template <typename R, typename C>
    static R accept(ASTPtr && node, ASTVisitor<R, C> & visitor, C & context)
    {
        return accept(node, visitor, context);
    }

    template <typename R, typename C>
    static R accept(ASTPtr & node, ASTVisitor<R, C> & visitor, C & context)
    {
        if (++visitor.level > visitor.max_level)
            throw Exception(ErrorCodes::TOO_DEEP_RECURSION, "Too deep recursion");
        SCOPE_EXIT({ --visitor.level; });

        switch(getAstType(node))
        {
#define VISITOR_DEF(TYPE) \
        case ASTType::TYPE: \
        { \
            return visitor.visit##TYPE(node, context); \
        }
        APPLY_AST_TYPES(VISITOR_DEF)
#undef VISITOR_DEF
            default:
                return visitor.visitNode(node, context);
        }
    }

    template <typename R, typename C>
    static R accept(const ConstASTPtr & node, ConstASTVisitor<R, C> & visitor, C & context)
    {
        if (++visitor.level > visitor.max_level)
            throw Exception(ErrorCodes::TOO_DEEP_RECURSION, "Too deep recursion");
        SCOPE_EXIT({ --visitor.level; });

        switch(getAstType(node))
        {
#define VISITOR_DEF(TYPE) \
        case ASTType::TYPE: \
        { \
            return visitor.visit##TYPE(node, context); \
        }
        APPLY_AST_TYPES(VISITOR_DEF)
#undef VISITOR_DEF
            default:
                return visitor.visitNode(node, context);
        }
    }
};

}
