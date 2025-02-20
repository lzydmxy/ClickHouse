 #include <Query/Parsers/ParserExplainQueryExt.h>

 #include <Query/Parsers/ASTExplainQueryExt.h>
 #include <Parsers/CommonParsers.h>
 #include <Parsers/ParserCreateQuery.h>
 #include <Parsers/ParserSelectWithUnionQuery.h>
 #include <Parsers/ParserInsertQuery.h>
 #include <Parsers/ParserSetQuery.h>
 #include <Parsers/ParserQuery.h>
 #include <Parsers/ParserSystemQuery.h>
 
 namespace DB
 {
 

    bool ParserExplainQueryExt::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
    {
        ASTExplainQueryExt::ExplainKind kind;
    
        ParserKeyword s_ast(Keyword::AST);
        ParserKeyword s_explain(Keyword::EXPLAIN);
        ParserKeyword s_syntax(Keyword::SYNTAX);
        ParserKeyword s_query_tree(Keyword::QUERY_TREE);
        ParserKeyword s_pipeline(Keyword::PIPELINE);
        ParserKeyword s_plan(Keyword::PLAN);
        ParserKeyword s_estimates(Keyword::ESTIMATE);
        ParserKeyword s_table_override(Keyword::TABLE_OVERRIDE);
        ParserKeyword s_current_transaction(Keyword::CURRENT_TRANSACTION);
    
        if (s_explain.ignore(pos, expected))
        {
            kind = ASTExplainQueryExt::ExplainKind::QueryPlan;
    
            if (s_ast.ignore(pos, expected))
                kind = ASTExplainQueryExt::ExplainKind::ParsedAST;
            else if (s_syntax.ignore(pos, expected))
                kind = ASTExplainQueryExt::ExplainKind::AnalyzedSyntax;
            // else if (s_query_tree.ignore(pos, expected))
            //     kind = ASTExplainQueryExt::ExplainKind::QueryTree;
            else if (s_pipeline.ignore(pos, expected))
                kind = ASTExplainQueryExt::ExplainKind::QueryPipeline;
            else if (s_plan.ignore(pos, expected))
                kind = ASTExplainQueryExt::ExplainKind::QueryPlan;
            // else if (s_estimates.ignore(pos, expected))
            //     kind = ASTExplainQueryExt::ExplainKind::QueryEstimates;
            // else if (s_table_override.ignore(pos, expected))
            //     kind = ASTExplainQueryExt::ExplainKind::TableOverride;
            // else if (s_current_transaction.ignore(pos, expected))
            //     kind = ASTExplainQueryExt::ExplainKind::CurrentTransaction;
        }
        else
            return false;
    
        auto explain_query = std::make_shared<ASTExplainQuery>(kind);
    
        {
            ASTPtr settings;
            ParserSetQuery parser_settings(true);
    
            auto begin = pos;
            if (parser_settings.parse(pos, settings, expected))
                explain_query->setSettings(std::move(settings));
            else
                pos = begin;
        }
    
        ParserCreateTableQuery create_p;
        ParserSelectWithUnionQuery select_p;
        ParserInsertQuery insert_p(end, allow_settings_after_format_in_insert);
        ParserSystemQuery system_p;
        ASTPtr query;
        if (kind == ASTExplainQueryExt::ExplainKind::ParsedAST)
        {
            ParserQuery p(end, allow_settings_after_format_in_insert);
            if (p.parse(pos, query, expected))
                explain_query->setExplainedQuery(std::move(query));
            else
                return false;
        }
        else if (kind == ASTExplainQueryExt::ExplainKind::TableOverride)
        {
            ASTPtr table_function;
            if (!ParserFunction(true, true).parse(pos, table_function, expected))
                return false;
            ASTPtr table_override;
            if (!ParserTableOverrideDeclaration(false).parse(pos, table_override, expected))
                return false;
            explain_query->setTableFunction(table_function);
            explain_query->setTableOverride(table_override);
        }
        else if (kind == ASTExplainQueryExt::ExplainKind::QueryTree)
        {
            if (select_p.parse(pos, query, expected))
                explain_query->setExplainedQuery(std::move(query));
            else
                return false;
        }
        else if (kind == ASTExplainQueryExt::ExplainKind::CurrentTransaction)
        {
            /// Nothing to parse
        }
        else if (select_only)
        {
            if (select_p.parse(pos, query, expected))
                explain_query->setExplainedQuery(std::move(query));
            else
                return false;
        }
        else if (select_p.parse(pos, query, expected) ||
            create_p.parse(pos, query, expected) ||
            insert_p.parse(pos, query, expected) ||
            system_p.parse(pos, query, expected))
        {
            explain_query->setExplainedQuery(std::move(query));
        }
        else
        {
            return false;
        }
    
        node = std::move(explain_query);
        return true;
    }

}
