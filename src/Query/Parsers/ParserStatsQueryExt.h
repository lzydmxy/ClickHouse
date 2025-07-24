#pragma once

#include <Parsers/ASTIdentifier.h>
#include <Query/Parsers/ASTStatsQueryExt.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/parseDatabaseAndTableName.h>

namespace DB
{

bool parseStatsQueryKind(IParser::Pos & pos, Expected & expected, StatsQueryKind & kind);

/** Query like this:
  * (SHOW | DROP) (STATS | TABLE_STATS | COLUMN_STATS) (ALL | [db_name.]table_name) [AT COLUMN column_name] [ON CLUSTER cluster]
  *
  * or:
  * CREATE (STATS | TABLE_STATS | COLUMN_STATS) (ALL | [db_name.]table_name) [AT COLUMN column_name]
  *                                                                          [ON CLUSTER cluster]
  *                                                                          [PARTITION partition | PARTITION ID 'partition_id']
  *                                                                          [WITH NUM BUCKETS|TOPN|SAMPLES]
  */
template <typename ParserName, typename QueryAstClass, typename QueryInfo>
class ParserStatsQueryBaseExt : public IParserBase
{
public:
    [[nodiscard]] const char * getName() const override { return ParserName::Name; }
    using QueryAst = QueryAstClass;

protected:
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        ParserKeyword s_query_prefix=ParserKeyword::createDeprecated(QueryInfo::QueryPrefix);
        ParserKeyword s_all=ParserKeyword::createDeprecated("ALL");
        ParserKeyword s_on=ParserKeyword::createDeprecated("ON");
        ParserToken open(TokenType::OpeningRoundBracket);
        ParserToken close(TokenType::ClosingRoundBracket);
        ParserIdentifier p_column_name;

        auto query = std::make_shared<QueryAstClass>();

        if (!s_query_prefix.ignore(pos, expected))
            return false;

        if (!parseStatsQueryKind(pos, expected, query->kind))
            return false;

        if constexpr (std::is_same_v<QueryInfo, CreateStatsQueryInfoExt>)
        {
            // IF NOT EXISTS is valid only for create
            ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
            if (s_if_not_exists.ignore(pos, expected))
                query->if_not_exists = true;
        }

        if (s_all.ignore(pos, expected))
        {
            // only for compatibility
            query->any_database = false;
            query->any_table = true;
        }
        else if (!parseDatabaseAndTableNameOrAsterisks(pos, expected, query->database, query->any_database, query->table, query->any_table))
        {
            return false;
        }

        if (!query->any_table)
        {
            if (open.ignore(pos, expected))
            {
                // parse columns when given table
                auto parse_id = [&query, &pos, &expected] {
                    ASTPtr identifier;
                    if (!ParserIdentifier(true).parse(pos, identifier, expected))
                        return false;

                    query->columns.emplace_back(getIdentifierName(identifier));
                    return true;
                };

                if (!ParserList::parseUtil(pos, expected, parse_id, false))
                    return false;

                if (!close.ignore(pos, expected))
                    return false;
            }
        }

        if (s_on.ignore(pos, expected))
        {
            if (!ASTQueryWithOnCluster::parse(pos, query->cluster, expected))
                return false;
        }

        if (!parseSuffix(pos, *query, expected))
            return false;

        node = query;
        return true;
    }

    // only for show/drop stats
    virtual bool parseSuffix(Pos & pos, QueryAst & node, Expected & expected)
    {
        ParserKeyword s_in(Keyword::IN);
        ParserKeyword s_catalog=ParserKeyword::createDeprecated("CATALOG");
        ParserKeyword s_cache=ParserKeyword::createDeprecated("CACHE");

        auto query = &node;
        if (s_in.ignore(pos, expected))
        {
            if (s_catalog.ignore(pos, expected))
            {
                query->cache_policy = StatisticsCachePolicy::Catalog;
                return true;
            }
            else if (s_cache.ignore(pos, expected))
            {
                query->cache_policy = StatisticsCachePolicy::Cache;
                return true;
            }
            else
            {
                return false;
            }
        }
        return true;
    }
};

struct CreateStatsParserName
{
    static constexpr auto Name = "Create stats query";
};

struct ShowStatsParserName
{
    static constexpr auto Name = "Show stats query";
};

struct DropStatsParserName
{
    static constexpr auto Name = "Drop stats query";
};

using ParserShowStatsQueryExt = ParserStatsQueryBaseExt<ShowStatsParserName, ASTShowStatsQueryExt, ShowStatsQueryInfoExt>;
using ParserDropStatsQueryExt = ParserStatsQueryBaseExt<DropStatsParserName, ASTDropStatsQueryExt, DropStatsQueryInfoExt>;

class ParserCreateStatsQueryExt : public ParserStatsQueryBaseExt<CreateStatsParserName, ASTCreateStatsQueryExt, CreateStatsQueryInfoExt>
{
protected:
    bool parseSuffix(Pos & pos, QueryAst & node, Expected & expected) override;
};
}
