#include <Query/Parsers/parseDatabaseAndTableNameExt.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>


namespace DB
{

bool parseDatabaseAndTableNameExt(IParser::Pos & pos, Expected & expected, String & database_str, String & table_str, bool rewrite_db)
{
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier table_parser;

    ASTPtr database;
    ASTPtr table;

    database_str = "";
    table_str = "";

    if (!table_parser.parse(pos, database, expected))
        return false;

    if (s_dot.ignore(pos))
    {
        if (!table_parser.parse(pos, table, expected))
        {
            database_str = "";
            return false;
        }
        
        //if (rewrite_db)
        //    tryRewriteCnchDatabaseName(database, pos.getContext());

        tryGetIdentifierNameInto(database, database_str);
        tryGetIdentifierNameInto(table, table_str);
    }
    else
    {
        database_str = "";
        tryGetIdentifierNameInto(database, table_str);
    }

    return true;
}


bool parseDatabaseAndTableNameOrAsterisksExt(IParser::Pos & pos, Expected & expected, ASTPtr & database, bool & any_database, ASTPtr & table, bool & any_table)
{
    return IParserBase::wrapParseImpl(pos, [&]
    {
        if (ParserToken{TokenType::Asterisk}.ignore(pos, expected))
        {
            auto pos_before_dot = pos;
            if (ParserToken{TokenType::Dot}.ignore(pos, expected)
                    && ParserToken{TokenType::Asterisk}.ignore(pos, expected))
            {
                /// *.*
                any_database = true;
                database = nullptr;
                any_table = true;
                table = nullptr;
                return true;
            }

            /// *
            pos = pos_before_dot;
            any_database = false;
            database = nullptr;  // TODO wujianchao use default database?
            any_table = true;
            table = nullptr;
            return true;
        }

        ASTPtr ast_db;
        ASTPtr ast_tb;
        ParserIdentifier identifier_parser;
        if (identifier_parser.parse(pos, ast_db, expected))
        {
            auto pos_before_dot = pos;
            if (ParserToken{TokenType::Dot}.ignore(pos, expected))
            {
                if (ParserToken{TokenType::Asterisk}.ignore(pos, expected))
                {
                    /// db.*
                    any_database = false;
                    database = ast_db;
                    any_table = true;
                    table= nullptr;
                    return true;
                }
                else if (identifier_parser.parse(pos, ast_tb, expected))
                {
                    /// db.table
                    any_database = false;
                    database = ast_db;
                    any_table = false;
                    table = ast_tb;
                    return true;
                }
            }

            /// table
            pos = pos_before_dot;
            any_database = false;
            database = nullptr;  // TODO wujianchao use default database?
            any_table = false;
            table = ast_db;
            return true;
        }

        return false;
    });
}

bool parseDatabaseExt(IParser::Pos & pos, Expected & expected, ASTPtr & database)
{
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier identifier_parser;

    if (!identifier_parser.parse(pos, database, expected))
        return false;
    return true;
}

}
