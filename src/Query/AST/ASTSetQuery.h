#pragma once

#include <Query/AST/IAST.h>
#include <Query/Common/SettingsChanges.h>


namespace JDDB
{

// using DB::SettingsChanges;
/** SET query
  */
class ASTSetQuery : public IAST
{
public:
    bool is_standalone = true; /// If false, this AST is a part of another query, such as SELECT.

    SettingsChanges changes;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "Set"; }

    ASTType getType() const override { return ASTType::ASTSetQuery; }

    DB::ASTPtr clone() const override { return std::make_shared<ASTSetQuery>(*this); }

    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    void serialize(WriteBuffer & buf) const override;
    void deserializeImpl(ReadBuffer & buf) override;
    static ASTPtr deserialize(ReadBuffer & buf);
};

}
