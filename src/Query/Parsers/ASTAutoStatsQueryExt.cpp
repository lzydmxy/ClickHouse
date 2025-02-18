#include <sstream>

#include <Query/Parsers/ASTAutoStatsQueryExt.h>
#include <Common/FieldVisitorToString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

using QueryPrefix = ASTAutoStatsQueryExt::QueryPrefix;
static String queryPrefixToString(QueryPrefix prefix)
{
    switch (prefix)
    {
        case QueryPrefix::Alter:
            return "ALTER";
        case QueryPrefix::Create:
            return "CREATE";
        case QueryPrefix::Drop:
            return "DROP";
        case QueryPrefix::Show:
            return "SHOW";
    }
}

void ASTAutoStatsQueryExt::formatQueryImpl(const FormatSettings & s, FormatState &, FormatStateStacked) const
{
    s.ostr << (s.hilite ? hilite_keyword : "") << queryPrefixToString(prefix) << " AUTO STATS" << (s.hilite ? hilite_none : "");
    if (prefix == QueryPrefix::Alter)
    {
        // DO NOTHING
    }
    else
    {
        s.ostr << " ";
        if (any_database)
            s.ostr << "*.";
        else if (!getDatabase().empty())
            s.ostr << backQuoteIfNeed(getDatabase()) << ".";
        s.ostr << (any_table ? "*" : backQuoteIfNeed(getTable()));
    }

    if (settings_changes_opt)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << " WITH " << (s.hilite ? hilite_none : "");
        bool is_first = true;
        for (auto [k, v] : settings_changes_opt.value())
        {
            if (!is_first)
            {
                s.ostr << ", ";
            }
            is_first = false;

            s.ostr << k << "=" << applyVisitor(FieldVisitorToString(), v);
        }
    }
}
}
