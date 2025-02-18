#pragma once

#include <Parsers/ASTColumnDeclaration.h>

namespace DB
{

/** Name, type, default-specifier, default-expression, comment-expression.
 *  The type is optional if default-expression is specified.
 */
class ASTColumnDeclarationExt : public ASTColumnDeclaration
{
public:
    std::optional<bool> unsigned_modifier;
    ASTPtr on_update_expression;
    bool auto_increment;
    bool mysql_primary_key;
    /// For partial update, this means the imported data will only be replaced when it is of non-null value
    /// We did not add this information to flags because it is only used on the write side and does not need to be serialized to the part.
    bool replace_if_not_null = false;
    UInt16 flags;

    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & format_settings, FormatState & state, FormatStateStacked frame) const override;
};

}
