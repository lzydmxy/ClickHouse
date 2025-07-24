#pragma once

#include <Parsers/IParser.h>

namespace DB
{

/// Parses [db.]name
bool parseDatabaseAndTableNameExt(IParser::Pos & pos, Expected & expected, String & database_str, String & table_str, bool rewrite_db = true);

/// Parses [db.]name or [db.]* or [*.]*
bool parseDatabaseAndTableNameOrAsterisksExt(IParser::Pos & pos, Expected & expected, ASTPtr & database, bool & any_database, ASTPtr & table, bool & any_table);

bool parseDatabaseExt(IParser::Pos & pos, Expected & expected, ASTPtr & database_str);

}
