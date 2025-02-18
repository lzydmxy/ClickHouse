#include <Query/Parsers/ASTColumnDeclarationExt.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTColumnDeclarationExt::clone() const
{
    auto res = std::make_shared<ASTColumnDeclarationExt>(*this);

    auto baseClonePtr = ASTColumnDeclaration::clone();
    *static_cast<ASTColumnDeclaration*>(res.get()) = *dynamic_cast<ASTColumnDeclaration*>(baseClonePtr.get());

    if (on_update_expression)
    {
        res->on_update_expression = on_update_expression->clone();
        res->children.push_back(res->on_update_expression);
    }

    if (replace_if_not_null)
        res->replace_if_not_null = replace_if_not_null;

    return res;
}

void ASTColumnDeclarationExt::formatImpl(const FormatSettings & format_settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    /// We have to always backquote column names to avoid ambiguouty with INDEX and other declarations in CREATE query.
    format_settings.ostr << backQuote(name);

    if (type)
    {
        format_settings.ostr << ' ';

        FormatStateStacked type_frame = frame;
        type_frame.indent = 0;

        type->formatImpl(format_settings, state, type_frame);


        if (unsigned_modifier)
        {
            format_settings.ostr << ' ' << (format_settings.hilite ? hilite_keyword : "")
                        << (*unsigned_modifier ? "UNSIGNED" : "SIGNED ") << (format_settings.hilite ? hilite_none : "");
        }
    }

    if (null_modifier)
    {
        format_settings.ostr << ' ' << (format_settings.hilite ? hilite_keyword : "")
                      << (*null_modifier ? "" : "NOT ") << "NULL" << (format_settings.hilite ? hilite_none : "");
    }

    if (default_expression)
    {
        format_settings.ostr << ' ' << (format_settings.hilite ? hilite_keyword : "") << default_specifier << (format_settings.hilite ? hilite_none : "");
        if (!ephemeral_default)
        {
            format_settings.ostr << ' ';
            default_expression->formatImpl(format_settings, state, frame);
        }
    }

    if (on_update_expression)
    {
        format_settings.ostr << ' ' << (format_settings.hilite ? hilite_keyword : "") << "ON UPDATE" << (format_settings.hilite ? hilite_none : "") << ' ';
        on_update_expression->formatImpl(format_settings, state, frame);
    }

    if (replace_if_not_null)
    {
        format_settings.ostr << ' ' << (format_settings.hilite ? hilite_keyword : "") << "REPLACE_IF_NOT_NULL"  << (format_settings.hilite ? hilite_none : "");
    }

    if (comment)
    {
        format_settings.ostr << ' ' << (format_settings.hilite ? hilite_keyword : "") << "COMMENT" << (format_settings.hilite ? hilite_none : "") << ' ';
        comment->formatImpl(format_settings, state, frame);
    }

    if (codec)
    {
        format_settings.ostr << ' ';
        codec->formatImpl(format_settings, state, frame);
    }

    if (stat_type)
    {
        format_settings.ostr << ' ';
        stat_type->formatImpl(format_settings, state, frame);
    }

    if (ttl)
    {
        format_settings.ostr << ' ' << (format_settings.hilite ? hilite_keyword : "") << "TTL" << (format_settings.hilite ? hilite_none : "") << ' ';
        ttl->formatImpl(format_settings, state, frame);
    }

    if (collation)
    {
        format_settings.ostr << ' ' << (format_settings.hilite ? hilite_keyword : "") << "COLLATE" << (format_settings.hilite ? hilite_none : "") << ' ';
        collation->formatImpl(format_settings, state, frame);
    }

    if (settings)
    {
        format_settings.ostr << ' ' << (format_settings.hilite ? hilite_keyword : "") << "SETTINGS" << (format_settings.hilite ? hilite_none : "") << ' ' << '(';
        settings->formatImpl(format_settings, state, frame);
        format_settings.ostr << ')';
    }
}

}
