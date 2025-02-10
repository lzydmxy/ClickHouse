#pragma once

#include <Parsers/IAST_fwd.h>
#include <Common/TypePromotion.h>
#include <Core/Settings.h>
#include <IO/WriteBufferFromString.h>

#include <algorithm>
#include <set>

class SipHash;


namespace JDDB
{
using DB::Exception;
using DB::ASTPtr;
using DB::ASTs;
using DB::IdentifierQuotingStyle;


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

using IdentifierNameSet = std::set<String>;

class WriteBuffer;
class ReadBuffer;

#define APPLY_AST_TYPES(M) \
    M(ASTAlterQuery) \
    M(ASTDeleteQuery) \
    M(ASTAlterCommand) \
    M(ASTAssignment) \
    M(ASTAsterisk) \
    M(ASTAlterDiskCacheQuery) \
    M(ASTCheckQuery) \
    M(ASTColumnDeclaration) \
    M(ASTColumnsMatcher) \
    M(ASTColumnsApplyTransformer) \
    M(ASTColumnsExceptTransformer) \
    M(ASTColumnsReplaceTransformer) \
    M(ASTConstraintDeclaration) \
    M(ASTForeignKeyDeclaration) \
    M(ASTUniqueNotEnforcedDeclaration) \
    M(ASTDataType) \
    M(ASTStorage) \
    M(ASTColumns) \
    M(ASTCreateQuery) \
    M(ASTCreateQuotaQuery) \
    M(ASTCreateRoleQuery) \
    M(ASTCreateRowPolicyQuery) \
    M(ASTCreateSettingsProfileQuery) \
    M(ASTCreateUserQuery) \
    M(ASTDictionaryLifetime) \
    M(ASTDictionaryLayout) \
    M(ASTDictionaryRange) \
    M(ASTDictionarySettings) \
    M(ASTDictionary) \
    M(ASTDictionaryAttributeDeclaration) \
    M(ASTDropAccessEntityQuery) \
    M(ASTDropQuery) \
    M(ASTExplainQuery) \
    M(ASTExpressionList) \
    M(ASTExternalDDLQuery) \
    M(ASTFunction) \
    M(ASTFunctionWithKeyValueArguments) \
    M(ASTGrantQuery) \
    M(ASTIdentifier) \
    M(ASTIndexDeclaration) \
    M(ASTInsertQuery) \
    M(ASTKillQueryQuery) \
    M(ASTLiteral) \
    M(ASTNameTypePair) \
    M(ASTOptimizeQuery) \
    M(ASTOrderByElement) \
    M(ASTPair) \
    M(ASTPartition) \
    M(ASTProjectionDeclaration) \
    M(ASTProjectionSelectQuery) \
    M(ASTQualifiedAsterisk) \
    M(ASTQueryParameter) \
    M(ASTQueryWithOutput) \
    M(ASTQueryWithTableAndOutput) \
    M(ASTRefreshQuery) \
    M(ASTRenameQuery) \
    M(ASTRolesOrUsersSet) \
    M(ASTRowPolicyName) \
    M(ASTRowPolicyNames) \
    M(ASTSampleRatio) \
    M(ASTSelectQuery) \
    M(ASTSelectWithUnionQuery) \
    M(ASTSetQuery) \
    M(ASTSetSensitiveQuery) \
    M(ASTSetRoleQuery) \
    M(ASTSettingsProfileElement) \
    M(ASTSettingsProfileElements) \
    M(ASTShowAccessEntitiesQuery) \
    M(ASTShowCreateAccessEntityQuery) \
    M(ASTShowGrantsQuery) \
    M(ASTShowTablesQuery) \
    M(ASTSubquery) \
    M(ASTSystemQuery) \
    M(ASTTableIdentifier) \
    M(ASTTableExpression) \
    M(ASTTableJoin) \
    M(ASTArrayJoin) \
    M(ASTTablesInSelectQueryElement) \
    M(ASTTablesInSelectQuery) \
    M(ASTTTLElement) \
    M(ASTUseQuery) \
    M(ASTSwitchQuery) \
    M(ASTUserNameWithHost) \
    M(ASTUserNamesWithHost) \
    M(ASTWatchQuery) \
    M(ASTWindowDefinition) \
    M(ASTWithElement) \
    M(ASTFieldReference) \
    M(ASTCreateStatsQuery) \
    M(ASTDropStatsQuery) \
    M(ASTShowStatsQuery) \
    M(ASTAutoStatsQuery) \
    M(ASTCreateBinding) \
    M(ASTShowBindings) \
    M(ASTDropBinding) \
    M(ASTAdviseQuery) \
    M(ASTSelectIntersectExceptQuery) \
    M(ASTWindowListElement) \
    M(ASTTEALimit) \
    M(ASTDumpQuery) \
    M(ASTReproduceQuery) \
    M(ASTPartToolKit) \
    M(ASTQuantifiedComparison) \
    M(ASTTableColumnReference) \
    M(ASTUpdateQuery) \
    M(ASTPreparedParameter) \
    M(ASTCreatePreparedStatementQuery) \
    M(ASTExecutePreparedStatementQuery) \
    M(ASTShowPreparedStatementQuery) \
    M(ASTDropPreparedStatementQuery) \
    M(ASTBitEngineConstraintDeclaration) \
    M(ASTStorageAnalyticalMySQL) \
    M(ASTCreateQueryAnalyticalMySQL) \
    M(ASTClusterByElement)
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

    ASTPtr ptr() { return shared_from_this(); }

    /// Get hash code, identifying this element and its subtree.
    using Hash = std::pair<UInt64, UInt64>;
    Hash getTreeHash() const;
    void updateTreeHash(SipHash & hash_state) const;
//    virtual void updateTreeHashImpl(SipHash & hash_state) const;

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
        for (ASTPtr & current_child: children)
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
    ASTs & getChildren() { return children; }
    void replaceChildren(ASTs & children_) { children = std::move(children_); }

    /// Convert to a string.

    /// Format settings.
    struct FormatSettings
    {
        WriteBuffer & ostr;
        bool remove_tenant_id = false;
        bool hilite = false;
        bool one_line;
        bool always_quote_identifiers = false;
        bool without_alias = false;
        IdentifierQuotingStyle identifier_quoting_style = IdentifierQuotingStyle::Backticks;
//        DialectType dialect_type = DialectType::CLICKHOUSE;
        bool show_secrets = true; /// Show secret parts of the AST (e.g. passwords, encryption keys).

        // Newline or whitespace.
        char nl_or_ws;

        FormatSettings(WriteBuffer & ostr_, bool one_line_, bool without_alias_ = false)
            : ostr(ostr_), one_line(one_line_), without_alias(without_alias_)
        {
            nl_or_ws = one_line ? ' ' : '\n';
        }

        FormatSettings(WriteBuffer & ostr_, const FormatSettings & other)
            : ostr(ostr_), hilite(other.hilite), one_line(other.one_line),
            always_quote_identifiers(other.always_quote_identifiers),
            identifier_quoting_style(other.identifier_quoting_style)
        {
            nl_or_ws = one_line ? ' ' : '\n';
        }

        void writeIdentifier(const String & name) const;
    };

    /// The state that is copied when each node is formatted. For example, nesting level.
    struct FormatStateStacked
    {
        UInt8 indent = 0;
        bool need_parens = false;
        bool expression_list_always_start_on_new_line = false;  /// Line feed and indent before expression list even if it's of single element.
        bool expression_list_prepend_whitespace = false; /// Prepend whitespace (if it is required)
        bool surround_each_list_element_with_parens = false;
        const IAST * current_select = nullptr;
    };

    void cloneChildren();

    virtual void serialize(WriteBuffer &) const { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implement serialize of {}", getID()); }
    virtual void deserializeImpl(ReadBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implement deserializeImpl AST"); }
    static ASTPtr deserialize(ReadBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implement deserialize AST"); }

public:
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

using ConstASTPtr = std::shared_ptr<const IAST>;
using ConstASTs = std::vector<ConstASTPtr>;
}
