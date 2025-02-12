#pragma once

#include <Parsers/IAST_fwd.h>
#include <Core/Settings.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>

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

using DB::IAST;
using DB::ASTPtr;
using DB::ASTs;
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
class IAST_EXT
{
public:
    virtual ~IAST_EXT() = default;
    IAST_EXT() = default;
    IAST_EXT(const IAST_EXT& other) = default;
    IAST_EXT& operator=(const IAST_EXT& other) = default;
    /// AST type, it's used for serialize/deserialize.
    virtual ASTType getType() const { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not support"); }

    /// Get hash code, identifying this element and its subtree.

    virtual IAST::Hash getTreeHash() const = 0;
    virtual void updateTreeHash(SipHash & hash_state) const = 0;

    virtual void toLowerCase() {}

    virtual void toUpperCase() {}

    void setOrReplaceAST(DB::ASTPtr & old_ast, const DB::ASTPtr & new_ast)
    {
        if (!new_ast)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to set or replace AST subtree with nullptr");

        if (old_ast == new_ast)
            return;

        /// set ast
        if (!old_ast)
        {
            old_ast = new_ast;
            getChildren().push_back(old_ast);
            return;
        }

        /// replace ast
        for (auto & current_child: getChildren())
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

    virtual  ASTs & getChildren() = 0;
    virtual void replaceChildren(DB::ASTs & children_) = 0;

    virtual void serialize(WriteBuffer &) const = 0;
    virtual void deserializeImpl(ReadBuffer &) = 0;
    static ASTPtr deserialize(ReadBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implement deserialize AST"); }
};

}
