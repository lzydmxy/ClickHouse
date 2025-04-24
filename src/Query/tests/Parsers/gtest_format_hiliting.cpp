#include <Parsers/HiliteComparator/HiliteComparator.h>
#include <Parsers/IAST.h>
#include <Parsers/parseQuery.h>
#include <Query/Parsers/ASTReplaceVisitor.h>
#include <Query/Parsers/ParserQueryExt.h>
#include <gtest/gtest.h>
#include <Common/quoteString.h>

using namespace DB;

String hiliteExt(const String & s, const char * hilite_type)
{
    return hilite_type + s + DB::IAST::hilite_none;
}

String keywordExt(const String & s)
{
    return hiliteExt(s, DB::IAST::hilite_keyword);
}

String identifierExt(const String & s)
{
    return hiliteExt(backQuoteIfNeed(s), DB::IAST::hilite_identifier);
}

String aliasExt(const String & s)
{
    return hiliteExt(backQuoteIfNeed(s), DB::IAST::hilite_alias);
}

String opExt(const String & s)
{
    return hiliteExt(s, DB::IAST::hilite_operator);
}

String functionExt(const String & s)
{
    return hiliteExt(s, DB::IAST::hilite_function);
}

String substitutionExt(const String & s)
{
    return hiliteExt(s, DB::IAST::hilite_substitution);
}


void compareExt(const String & expected, const String & query)
{
    using namespace DB;
    ParserQueryExt parser(query.data() + query.size());
    ASTPtr ast = parseQuery(parser, query, 0, 0, 0);
    // DB::ASTReplaceVisitor::replace(ast);
    WriteBufferFromOwnString write_buffer;
    IAST::FormatSettings settings(write_buffer, true, true);
    ast->format(settings);

    ASSERT_PRED2(HiliteComparator::are_equal_with_hilites_removed, expected, write_buffer.str());
    ASSERT_PRED2(HiliteComparator::are_equal_with_hilites_and_end_without_hilite, expected, write_buffer.str());
}

const std::vector<std::pair<std::string, std::string>> expected_and_query_pairs_ext = {
    // Simple select
    {keywordExt("SELECT") + " * " + keywordExt("FROM") + " " + identifierExt("table"), "select * from `table`"},

    // ASTWithElement
    {keywordExt("WITH ") + aliasExt("alias ") + " " + keywordExt("AS") + " (" + keywordExt("SELECT") + " * " + keywordExt("FROM") + " "
         + identifierExt("table") + ") " + keywordExt("SELECT") + " * " + keywordExt("FROM") + " " + identifierExt("table"),
     "with `alias ` as (select * from `table`) select * from `table`"},

    // ASTWithAlias
    {keywordExt("SELECT") + " " + identifierExt("a") + " " + opExt("+") + " 1 " + keywordExt("AS") + " " + aliasExt("b") + ", "
         + identifierExt("b"),
     "select a + 1 as b, b"},

    // ASTFunction
    {keywordExt("SELECT ") + "* " + keywordExt("FROM ") + functionExt("view(") + keywordExt("SELECT") + " * " + keywordExt("FROM ")
         + identifierExt("table") + functionExt(")"),
     "select * from view(select * from `table`)"},

    // ASTDictionaryAttributeDeclaration
    {keywordExt("CREATE DICTIONARY ") + identifierExt("name") + " " + "(`Name` " + functionExt("ClickHouseDataType")
         + keywordExt(" DEFAULT") + " '' " + keywordExt("EXPRESSION") + " " + functionExt("rand64()") + " " + keywordExt("IS_OBJECT_ID")
         + ")",
     "CREATE DICTIONARY name (`Name` ClickHouseDataType DEFAULT '' EXPRESSION rand64() IS_OBJECT_ID)"},

    // ASTDictionary, SOURCE keyword
    {keywordExt("CREATE DICTIONARY ") + identifierExt("name") + " " + "(`Name`" + " " + functionExt("ClickHouseDataType ")
         + keywordExt("DEFAULT") + " '' " + keywordExt("EXPRESSION") + " " + functionExt("rand64()") + " " + keywordExt("IS_OBJECT_ID")
         + ") " + keywordExt("SOURCE") + "(" + keywordExt("FILE") + "(" + keywordExt("PATH") + " 'path'))",
     "CREATE DICTIONARY name (`Name` ClickHouseDataType DEFAULT '' EXPRESSION rand64() IS_OBJECT_ID) "
     "SOURCE(FILE(PATH 'path'))"},

    // ASTKillQueryQuery
    {keywordExt("KILL QUERY ON CLUSTER") + " clustername " + keywordExt("WHERE") + " " + identifierExt("user") + opExt(" = ")
         + "'username' " + keywordExt("SYNC"),
     "KILL QUERY ON CLUSTER clustername WHERE user = 'username' SYNC"},

    // ASTCreateQuery
    {keywordExt("CREATE TABLE ") + identifierExt("name") + " " + keywordExt("AS (SELECT") + " *" + keywordExt(")") + " "
         + keywordExt("COMMENT") + " 'hello'",
     "CREATE TABLE name AS (SELECT *) COMMENT 'hello'"},
};


TEST(FormatHiliting, QueriesExt)
{
    for (const auto & [expected, query] : expected_and_query_pairs_ext)
        compareExt(expected, query);
}
