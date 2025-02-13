#pragma once

#include <Query/Parsers/IAST.h>
#include <Parsers/ASTDictionaryAttributeDeclaration.h>

namespace JDDB
{

/// AST for single dictionary attribute in dictionary DDL query
class ASTDictionaryAttributeDeclaration : public IAST_EXT, public DB::ASTDictionaryAttributeDeclaration
{
public:
    /// Is hierarchical attribute bidirectional
    bool bidirectional = false;

    String getID(char delim) const override { return "DictionaryAttributeDeclaration" + (delim + name); }

    ASTType getType() const override { return ASTType::ASTDictionaryAttributeDeclaration; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override { IAST::updateTreeHashImpl(hash_state, ignore_aliases); }
    Hash getTreeHash() const override { return IAST::getTreeHash(true); }
    void updateTreeHash(SipHash & hash_state) const override { IAST::updateTreeHash(hash_state, true); }

    void serialize(WriteBuffer & buf) const override;
    void deserializeImpl(ReadBuffer & buf) override;
    static ASTPtr deserialize(ReadBuffer & buf);

    ASTs & getChildren() override { return children; }
    void replaceChildren(DB::ASTs & children_) override { children = std::move(children_); }
};

}
