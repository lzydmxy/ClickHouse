#pragma once

#include <Parsers/IAST_fwd.h>
#include <Core/Settings.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <Query/AST/IAST_fwd.h>

#include <algorithm>
#include <set>

class SipHash;

namespace JDDB
{
using DB::Exception;
using DB::IdentifierQuotingStyle;
using DB::WriteBuffer;
using DB::ReadBuffer;
using DB::WriteBufferFromOwnString;
using DB::ReadBufferFromString;

class IAST;
using ConstASTPtr = std::shared_ptr<const IAST>;
using ConstASTs = std::vector<ConstASTPtr>;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

using IdentifierNameSet = std::set<String>;

#define APPLY_AST_TYPES(M) \
    M(ASTSetQuery)
#define ENUM_TYPE(ITEM) ITEM,

enum class ASTType : UInt8
{
    APPLY_AST_TYPES(ENUM_TYPE) UNDEFINED,
};

#undef ENUM_TYPE

inline String toString(ASTType type)
{
    switch (type)
    {
#define ENUM_TYPE(ITEM) \
    case ASTType::ITEM: \
        return #ITEM;
        APPLY_AST_TYPES(ENUM_TYPE)
#undef ENUM_TYPE
        default:
            return "UNDEFINED";
    }
}

using StringPair = std::pair<String, String>;
using StringPairs = std::vector<StringPair>;

/** Element of the syntax tree (hereinafter - directed acyclic graph with elements of semantics)
  */
class IAST : public DB::IAST
{
public:
    /// AST type, it's used for serialize/deserialize.
    virtual ASTType getType() const { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not support"); }

    DB::ASTPtr ptr() { return shared_from_this(); }

    /// Get hash code, identifying this element and its subtree.
    Hash getTreeHash() const { return DB::IAST::getTreeHash(true); }
    void updateTreeHash(SipHash & hash_state) const { DB::IAST::updateTreeHash(hash_state, true); }

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases = 0) const override { DB::IAST::updateTreeHashImpl(hash_state, true); }

    void dumpTree(WriteBuffer & ostr, size_t indent = 0) const;
    std::string dumpTree(size_t indent = 0) const;

    virtual void toLowerCase() {}

    virtual void toUpperCase() {}

    /** Check the depth of the tree.
      * If max_depth is specified and the depth is greater - throw an exception.
      * Returns the depth of the tree.
      */
    size_t checkDepth(size_t max_depth) const
    {
        return checkDepthImpl(max_depth, 0);
    }

    /** Get total number of tree elements
     */
    size_t size() const;

    void setOrReplaceAST(ASTPtr & old_ast, const ASTPtr & new_ast)
    {
        if (!new_ast)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to set or replace AST subtree with nullptr");

        if (old_ast == new_ast)
            return;

        /// set ast
        if (!old_ast)
        {
            old_ast = new_ast;
            children.push_back(old_ast);
            return;
        }

        /// replace ast
        for (auto & current_child: children)
        {
            if (current_child == old_ast)
            {
                current_child = new_ast;
                old_ast = new_ast;
                return;
            }
        }

        throw Exception(ErrorCodes::LOGICAL_ERROR, "AST subtree not found in children");
    }
    DB::ASTs & getChildren() { return children; }
    void replaceChildren(DB::ASTs & children_) { children = std::move(children_); }

    void cloneChildren();

    virtual void serialize(WriteBuffer &) const { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implement serialize of {}", getID()); }
    virtual void deserializeImpl(ReadBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implement deserializeImpl AST"); }
    static ASTPtr deserialize(ReadBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implement deserialize AST"); }

    /// For syntax highlighting.
    static const char * hilite_keyword;
    static const char * hilite_identifier;
    static const char * hilite_function;
    static const char * hilite_operator;
    static const char * hilite_alias;
    static const char * hilite_substitution;
    static const char * hilite_none;

protected:
    bool childrenHaveSecretParts() const;

private:
    size_t checkDepthImpl(size_t max_depth, size_t level) const;
};

}
