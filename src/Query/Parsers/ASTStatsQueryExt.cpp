#include <Query/Parsers/ASTStatsQueryExt.h>
#include <Common/FieldVisitorToString.h>

namespace DB
{

String formatStatsQueryKind(StatsQueryKind kind)
{
    if (kind == StatsQueryKind::TABLE_STATS)
        return "TABLE_STATS";
    else if (kind == StatsQueryKind::COLUMN_STATS)
        return "COLUMN_STATS";
    else if (kind == StatsQueryKind::ALL_STATS)
        return "STATS";
    else
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Not supported kind of stats query kind.");
}

ASTPtr ASTCreateStatsQueryExt::clone() const
{
    auto res = std::make_shared<ASTCreateStatsQueryExt>(*this);
    res->children.clear();

    if (partition)
    {
        res->partition = partition->clone();
        res->children.push_back(res->partition);
    }

    cloneOutputOptions(*res);
    return res;
}

void ASTCreateStatsQueryExt::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    formatQueryPrefix(settings, state, frame);

    if (if_not_exists)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " IF NOT EXISTS" << (settings.hilite ? hilite_none : "");
    }

    formatQueryMiddle(settings, state, frame);

    if (partition)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " PARTITION " << (settings.hilite ? hilite_none : "");
        partition->formatImpl(settings, state, frame);
    }

    if (sync_mode != SyncMode::Default)
    {
        String str = sync_mode == SyncMode::Sync ? " SYNC" : " ASYNC";
        settings.ostr << (settings.hilite ? hilite_keyword : "") << str << (settings.hilite ? hilite_none : "");
    }

    bool printed = false;
    auto printWithIfNeeded = [&printed, &settings] {
        if (!printed)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " WITH" << (settings.hilite ? hilite_none : "");
            printed = true;
        }
    };

    if (sample_type == SampleType::FullScan)
    {
        printWithIfNeeded();
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " FULLSCAN" << (settings.hilite ? hilite_none : "");
    }
    else if (sample_type == SampleType::Sample)
    {
        printWithIfNeeded();
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " SAMPLE" << (settings.hilite ? hilite_none : "");
        if (sample_rows)
        {
            settings.ostr << ' ' << *sample_rows;
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " ROWS" << (settings.hilite ? hilite_none : "");
        }

        if (sample_ratio)
        {
            settings.ostr << ' ' << *sample_ratio;
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " RATIO" << (settings.hilite ? hilite_none : "");
        }
    }

    if (settings_changes_opt)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " WITH " << (settings.hilite ? hilite_none : "");
        bool is_first = true;
        for (auto [k, v] : settings_changes_opt.value())
        {
            if (!is_first)
            {
                settings.ostr << ", ";
            }
            is_first = false;

            settings.ostr << k << "=" << applyVisitor(FieldVisitorToString(), v);
        }
    }
}

}
