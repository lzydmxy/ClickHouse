#pragma once

#include <Parsers/ASTWithAlias.h>
#include <Parsers/IAST.h>

namespace DB
{

/// this AST is only used by optimizer.
class ASTFieldReferenceExt : public ASTWithAlias
{
public:
    size_t field_index;
    String field_name;

    explicit ASTFieldReferenceExt(size_t field_index_) : field_index(field_index_) {}

    String getID(char delim) const override { return "FieldRef" + (delim + std::to_string(field_index)); }

    ASTPtr clone() const override;

    void setFieldName(String field_name_) { field_name = field_name_; }
protected:
    void formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void appendColumnNameImpl(WriteBuffer & ostr) const override;
};

}
