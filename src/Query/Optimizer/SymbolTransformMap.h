#pragma once

#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage.h>
#include <Query/Parsers/ASTHelper.h>
#include <Query/Optimizer/SimpleExpressionRewriter.h>
#include <Query/Analyzer/ASTEquals.h>
#include <Query/Common/SymbolsExtractor.h>

#include <optional>
#include <unordered_map>

namespace DB
{
class PlanNodeBase;
using PlanNodePtr = std::shared_ptr<PlanNodeBase>;
using PlanNodes = std::vector<PlanNodePtr>;
using String = std::string;
using UInt32 = uint32_t;
using PlanNodeId = UInt32;

class SymbolTransformMap
{
public:
    static std::optional<SymbolTransformMap> buildFrom(PlanNodeBase & plan, std::optional<PlanNodeId> stop_node = std::nullopt);

    ASTPtr inlineReferences(const ConstASTPtr & expression) const;

    ASTPtr inlineReferences(const String & symbol) const { return inlineReferences(std::make_shared<ASTIdentifier>(symbol)); }

    String toString() const;

private:
    bool addSymbolMapping(const String & symbol, ConstASTPtr expr);

    std::unordered_map<String, ConstASTPtr> symbol_to_expressions;

    mutable std::unordered_map<String, ConstASTPtr> expression_lineage;

    class Visitor;
    class Rewriter;
};

class SymbolTranslationMap
{
public:
    void addTranslation(ASTPtr ast, String name)
    {
        translation.emplace(std::move(ast), std::move(name));
    }
    // rewrite table column to ASTColumnReference before adding translation
    void addStorageTranslation(ASTPtr ast, String name, const IStorage * storage, UInt32 unique_id);
    std::optional<String> tryGetTranslation(const ASTPtr & expr) const;
    ASTPtr translate(ASTPtr ast) const
    {
        return translateImpl(ast);
    }

private:
    ASTMap<String> translation;

    ASTPtr translateImpl(ASTPtr ast) const;
};

class IdentifierToColumnReference : public SimpleExpressionRewriter<Void>
{
public:
    static ASTPtr rewrite(const IStorage * storage, UInt32 unique_id, ASTPtr ast, bool clone = true);

private:
    const IStorage * storage;
    UInt32 unique_id;
    StorageMetadataPtr storage_metadata;

public:
    IdentifierToColumnReference(const IStorage * storage_, UInt32 unique_id_);
    ASTPtr visitASTIdentifier(ASTPtr & node, Void & context) override;
};

class ColumnReferenceToIdentifier : public SimpleExpressionRewriter<Void>
{
public:
    static ASTPtr rewrite(ASTPtr ast, bool clone = true);
    ASTPtr visitASTTableColumnReference(ASTPtr & node, Void & context);
};

}
