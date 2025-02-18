#include <Query/Parsers/ASTFieldReferenceExt.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTFieldReferenceExt::clone() const
{
    return std::make_shared<ASTFieldReferenceExt>(*this);
}

void ASTFieldReferenceExt::formatImplWithoutAlias(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_identifier : "");
    settings.writeIdentifier("@" + std::to_string(field_index));
    settings.ostr << (settings.hilite ? hilite_none : "");
}

void ASTFieldReferenceExt::appendColumnNameImpl(WriteBuffer & ostr) const
{
    if (!field_name.empty()) 
        writeString(field_name, ostr);
    else    
        writeString("@" + std::to_string(field_index), ostr);
}

}
