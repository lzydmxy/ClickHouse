#include <IO/Operators.h>
#include <Query/Parsers/ASTDataTypeExt.h>


namespace DB
{

ASTDataTypeExt::ASTDataTypeExt(const ASTPtr & dt, bool nullable_)
    : data_type(dt), nullable(nullable_)
{
    children.push_back(dt);
}

ASTPtr ASTDataTypeExt::clone() const
{
    return std::make_shared<ASTDataTypeExt>(this->data_type->clone(), nullable);
}

void ASTDataTypeExt::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (nullable)
        settings.ostr << (settings.hilite ? hilite_function : "")
                      << "Nullable" << (settings.hilite ? hilite_none : "")
                      << "(";
    data_type->formatImpl(settings, state, frame);
    if (nullable)
        settings.ostr << ')';
}

}
