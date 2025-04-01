#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/ParserSetQuery.h>
#include <Query/Parsers/ParserStatsQueryExt.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Common/FieldVisitorConvertToNumber.h>

namespace DB
{

bool parseStatsQueryKind(IParser::Pos & pos, Expected & expected, StatsQueryKind & kind)
{
    ParserKeyword s_stats=ParserKeyword::createDeprecated("STATS");
    ParserKeyword s_table_stats=ParserKeyword::createDeprecated("TABLE_STATS");
    ParserKeyword s_column_stats=ParserKeyword::createDeprecated("COLUMN_STATS");

    if (s_stats.ignore(pos, expected))
        kind = StatsQueryKind::ALL_STATS;
    else if (s_table_stats.ignore(pos, expected))
        kind = StatsQueryKind::TABLE_STATS;
    else if (s_column_stats.ignore(pos, expected))
        kind = StatsQueryKind::COLUMN_STATS;
    else
        return false;

    return true;
}

template <typename SimpleWithSpecifierInfo>
class ParserCreateStatsQuerySimpleSpecifier : public IParserBase
{
public:
    [[nodiscard]] const char * getName() const override { return SimpleWithSpecifierInfo::Name; }

protected:
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        typename SimpleWithSpecifierInfo::ParserType number_p;
        ParserKeyword s_specifier=ParserKeyword::createDeprecated(SimpleWithSpecifierInfo::Specifier);

        if (!number_p.parse(pos, node, expected))
            return false;

        if (!s_specifier.ignore(pos, expected))
        {
            node = nullptr;
            return false;
        }

        return true;
    }
};

struct ParserCreateStatsQuerySampleRowsSpecifierInfo
{
    using ParserType = ParserUnsignedInteger;
    static constexpr auto Name = "CreateStat query sample rows specifier";
    static constexpr auto Specifier = "ROWS";
};

struct ParserCreateStatsQuerySampleRatioSpecifierInfo
{
    using ParserType = ParserNumber;
    static constexpr auto Name = "CreateStat query sample ratio specifier";
    static constexpr auto Specifier = "Ratio";
};

using ParserCreateStatsQuerySampleRowsSpecifier = ParserCreateStatsQuerySimpleSpecifier<ParserCreateStatsQuerySampleRowsSpecifierInfo>;
using ParserCreateStatsQuerySampleRatioSpecifier = ParserCreateStatsQuerySimpleSpecifier<ParserCreateStatsQuerySampleRatioSpecifierInfo>;

Int64 getValueFromUInt64Literal(const ASTPtr & node)
{
    if (auto * literal = node->as<ASTLiteral>())
    {
        UInt64 val = literal->value.safeGet<UInt64>();

        if (val > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Value exceed limit");

        return static_cast<Int64>(val);
    }
    else
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Not a literal node");
}

Float64 getValueFromNumberLiteral(const ASTPtr & node)
{
    if (auto * literal = node->as<ASTLiteral>())
    {
        try
        {
            double val = applyVisitor(FieldVisitorConvertToNumber<Float64>(), literal->value);
            return val;
        }
        catch (...)
        {
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Not a literal node");
        }
    }
    else
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Not a literal node");
}

bool ParserCreateStatsQueryExt::parseSuffix(Pos & pos, QueryAst & node, Expected & expected)
{
    ParserKeyword s_sync=ParserKeyword::createDeprecated("SYNC");
    ParserKeyword s_async=ParserKeyword::createDeprecated("ASYNC");
    ParserKeyword s_partition=ParserKeyword::createDeprecated("PARTITION");
    ParserKeyword s_with=ParserKeyword::createDeprecated("WITH");
    ParserToken s_comma(TokenType::Comma);
    ParserPartition partition_p;
    ParserNothing dummy_p;

    using SampleType = ASTCreateStatsQueryExt::SampleType;
    using SyncMode = ASTCreateStatsQueryExt::SyncMode;

    auto & create_stats_ast = node;

    if (s_partition.ignore(pos, expected))
    {
        ASTPtr partition;

        if (!partition_p.parse(pos, partition, expected))
            return false;

        create_stats_ast.partition = partition;
        create_stats_ast.children.push_back(create_stats_ast.partition);
    }

    if (s_sync.ignore(pos, expected))
    {
        create_stats_ast.sync_mode = SyncMode::Sync;
    }
    else if (s_async.ignore(pos, expected))
    {
        create_stats_ast.sync_mode = SyncMode::Async;
    }
    bool unfinished_with = false;
    if (s_with.ignore(pos, expected))
    {
        // old syntax, here just for compactibility
        auto parse_specifier = [&pos, &expected, &create_stats_ast] {
            ParserKeyword s_sample=ParserKeyword::createDeprecated("SAMPLE");
            ParserKeyword s_fullscan=ParserKeyword::createDeprecated("FULLSCAN");
            ASTPtr ast_ptr;

            if (s_fullscan.ignore(pos, expected))
            {
                if (create_stats_ast.sample_type != SampleType::Default)
                    return false; // duplicate
                create_stats_ast.sample_type = SampleType::FullScan;
                return true;
            }
            else if (s_sample.ignore(pos, expected))
            {
                if (create_stats_ast.sample_type != SampleType::Default)
                    return false; // duplicate

                create_stats_ast.sample_type = SampleType::Sample;
                ParserCreateStatsQuerySampleRowsSpecifier rows_p;
                ParserCreateStatsQuerySampleRatioSpecifier ratio_p;
                while (true)
                {
                    if (rows_p.parse(pos, ast_ptr, expected))
                    {
                        if (create_stats_ast.sample_rows)
                            return false; // duplicate
                        create_stats_ast.sample_rows = getValueFromUInt64Literal(ast_ptr);
                    }
                    else if (ratio_p.parse(pos, ast_ptr, expected))
                    {
                        if (create_stats_ast.sample_ratio)
                            return false; // duplicate
                        create_stats_ast.sample_ratio = getValueFromNumberLiteral(ast_ptr);
                    }
                    else
                    {
                        break;
                    }
                }
                return true;
            }
            else
                return false;
        };

        if (!ParserList::parseUtil(pos, expected, parse_specifier, dummy_p, false))
            unfinished_with = true;
    }

    if (unfinished_with || s_with.ignore(pos, expected))
    {
        // new syntax can support any settings in here
        SettingsChanges changes;
        while (true)
        {
            if (!changes.empty() && !s_comma.ignore(pos))
                break;
            changes.push_back(SettingChange{});
            if (!ParserSetQuery::parseNameValuePair(changes.back(), pos, expected))
                return false;
        }
        create_stats_ast.settings_changes_opt = changes;
    }

    return true;
}

}
