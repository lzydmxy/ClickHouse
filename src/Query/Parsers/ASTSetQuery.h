#pragma once

#include <Query/Parsers/IAST.h>
// #include <Query/Common/SettingsChanges.h>
#include <Parsers/ASTSetQuery.h>

namespace JDDB
{

/** SET query
  */
class ASTSetQuery : public IAST_EXT, public DB::ASTSetQuery
{
public:
    /** Get the text that identifies this element. */
    String getID(char) const override { return "JD_Set"; }

    ASTPtr clone() const override { return std::make_shared<ASTSetQuery>(*this); }

    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    /// The follow methods are newly added by ByConity.

    ASTType getType() const override { return ASTType::ASTSetQuery; }
    void serialize(WriteBuffer & buf) const override;
    void deserializeImpl(ReadBuffer & buf) override;
    static ASTPtr deserialize(ReadBuffer & buf);
    /// Get hash code, identifying this element and its subtree.

    Hash getTreeHash() const override { return IAST::getTreeHash(true); }
    void updateTreeHash(SipHash & hash_state) const override { IAST::updateTreeHash(hash_state, true); }

    ASTs & getChildren() override { return children; }
    void replaceChildren(DB::ASTs & children_) override { children = std::move(children_); }
};

}
