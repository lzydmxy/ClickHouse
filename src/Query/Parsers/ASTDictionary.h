#pragma once

#include <Query/Parsers/IAST.h>
#include <Parsers/ASTDictionary.h>

namespace JDDB
{

class ASTLiteral;

/// AST for external dictionary lifetime:
/// lifetime(min 10 max 100)
class ASTDictionaryLifetime : public IAST_EXT, public DB::ASTDictionaryLifetime
{
public:
    String getID(char) const override { return "Dictionary lifetime"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    ASTType getType() const override { return ASTType::ASTDictionaryLifetime; }

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override { IAST::updateTreeHashImpl(hash_state, ignore_aliases); }
    Hash getTreeHash() const override { return IAST::getTreeHash(true); }
    void updateTreeHash(SipHash & hash_state) const override { IAST::updateTreeHash(hash_state, true); }

    void serialize(WriteBuffer & buf) const override;
    void deserializeImpl(ReadBuffer & buf) override;
    static ASTPtr deserialize(ReadBuffer & buf);

    ASTs & getChildren() override { return children; }
    void replaceChildren(DB::ASTs & children_) override { children = std::move(children_); }

};

/// AST for external dictionary layout. Has name and contain single parameter
/// layout(type()) or layout(type(param value))
class ASTDictionaryLayout : public IAST_EXT, public DB::ASTDictionaryLayout
{
    using KeyValue = std::pair<std::string, ASTLiteral *>;
public:
    String getID(char) const override { return "Dictionary layout"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    void forEachPointerToChild(std::function<void(void**)> f) override
    {
        f(reinterpret_cast<void **>(&parameters));
    }

    ASTType getType() const override { return ASTType::ASTDictionaryLayout; }

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override { IAST::updateTreeHashImpl(hash_state, ignore_aliases); }
    Hash getTreeHash() const override { return IAST::getTreeHash(true); }
    void updateTreeHash(SipHash & hash_state) const override { IAST::updateTreeHash(hash_state, true); }

    void serialize(WriteBuffer & buf) const override;
    void deserializeImpl(ReadBuffer & buf) override;
    static ASTPtr deserialize(ReadBuffer & buf);

    ASTs & getChildren() override { return children; }
    void replaceChildren(DB::ASTs & children_) override { children = std::move(children_); }
};


/// AST for external range-hashed dictionary
/// Range bounded with two attributes from minimum to maximum
/// RANGE(min attr1 max attr2)
class ASTDictionaryRange : public IAST_EXT, public DB::ASTDictionaryRange
{
public:
    String getID(char) const override { return "Dictionary range"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    ASTType getType() const override { return ASTType::ASTDictionaryRange; }

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override { IAST::updateTreeHashImpl(hash_state, ignore_aliases); }
    Hash getTreeHash() const override { return IAST::getTreeHash(true); }
    void updateTreeHash(SipHash & hash_state) const override { IAST::updateTreeHash(hash_state, true); }

    void serialize(WriteBuffer & buf) const override;
    void deserializeImpl(ReadBuffer & buf) override;
    static ASTPtr deserialize(ReadBuffer & buf);

    ASTs & getChildren() override { return children; }
    void replaceChildren(DB::ASTs & children_) override { children = std::move(children_); }
};

class ASTDictionarySettings : public IAST_EXT, public DB::ASTDictionarySettings
{
public:
    String getID(char) const override { return "Dictionary settings"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    ASTType getType() const override { return ASTType::ASTDictionarySettings; }

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override { IAST::updateTreeHashImpl(hash_state, ignore_aliases); }
    Hash getTreeHash() const override { return IAST::getTreeHash(true); }
    void updateTreeHash(SipHash & hash_state) const override { IAST::updateTreeHash(hash_state, true); }

    void serialize(WriteBuffer & buf) const override;
    void deserializeImpl(ReadBuffer & buf) override;
    static ASTPtr deserialize(ReadBuffer & buf);

    ASTs & getChildren() override { return children; }
    void replaceChildren(DB::ASTs & children_) override { children = std::move(children_); }
};


/// AST contains all parts of external dictionary definition except attributes
class ASTDictionary : public IAST_EXT, public DB::ASTDictionary
{
public:
    String clickhouse_db;
    String clickhouse_tb;
    String clickhouse_query;
    String clickhouse_invalidate_query;

    String getID(char) const override { return "Dictionary definition"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;


    ASTType getType() const override { return ASTType::ASTDictionary; }

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
