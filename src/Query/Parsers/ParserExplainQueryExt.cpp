#include <Query/Parsers/ParserExplainQueryExt.h>

#include <Parsers/ASTSetQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserInsertQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Query/Common/OptimizerContext.h>
#include <Query/Parsers/ASTExplainQueryExt.h>


namespace DB
{

bool ParserExplainQueryExt::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!enable_optimizer)
        return false;

    ASTExplainQueryExt::ExplainKindExt kind;

    ParserKeyword s_ast(Keyword::AST);
    ParserKeyword s_explain(Keyword::EXPLAIN);
    ParserKeyword s_syntax(Keyword::SYNTAX);
    ParserKeyword s_pipeline(Keyword::PIPELINE);
    ParserKeyword s_plan(Keyword::PLAN);
    ParserKeyword s_element(Keyword::ELEMENT);
    ParserKeyword s_plansegment(Keyword::PLANSEGMENT);
    ParserKeyword s_opt_plan(Keyword::OPT_PLAN);
    ParserKeyword s_distributed(Keyword::DISTRIBUTED);
    ParserKeyword s_analyze(Keyword::ANALYZE);
    ParserKeyword s_trace(Keyword::TRACE_OPT);
    ParserKeyword s_rule(Keyword::RULE);
    ParserKeyword s_metadata(Keyword::METADATA);


    if (s_explain.ignore(pos, expected))
    {
        kind = ASTExplainQueryExt::QueryPlan;
        if (s_ast.ignore(pos, expected))
            kind = ASTExplainQueryExt::ExplainKindExt::ParsedAST;
        else if (s_syntax.ignore(pos, expected))
            kind = ASTExplainQueryExt::ExplainKindExt::AnalyzedSyntax;
        else if (s_pipeline.ignore(pos, expected))
            kind = ASTExplainQueryExt::ExplainKindExt::QueryPipeline;
        else if (s_plan.ignore(pos, expected))
            kind = ASTExplainQueryExt::ExplainKindExt::QueryPlan; //-V1048
        else if (s_element.ignore(pos, expected))
            kind = ASTExplainQueryExt::ExplainKindExt::QueryElement;
        else if (s_plansegment.ignore(pos, expected))
            kind = ASTExplainQueryExt::ExplainKindExt::PlanSegment;
        else if (s_opt_plan.ignore(pos, expected))
            kind = ASTExplainQueryExt::ExplainKindExt::OptimizerPlan;
        else if (s_distributed.ignore(pos, expected))
            kind = ASTExplainQueryExt::ExplainKindExt::Distributed;
        else if (s_analyze.ignore(pos, expected))
        {
            if (s_distributed.ignore(pos, expected))
                kind = ASTExplainQueryExt::ExplainKindExt::DistributedAnalyze;
            else if (s_pipeline.ignore(pos, expected))
                kind = ASTExplainQueryExt::ExplainKindExt::PipelineAnalyze;
            else
                kind = ASTExplainQueryExt::ExplainKindExt::LogicalAnalyze;
        }
        else if (s_trace.ignore(pos, expected))
        {
            if (s_rule.ignore(pos, expected))
                kind = ASTExplainQueryExt::ExplainKindExt::TraceOptimizerRule;
            else
                kind = ASTExplainQueryExt::ExplainKindExt::TraceOptimizer;
        }
        else if (s_metadata.ignore(pos, expected))
            kind = ASTExplainQueryExt::ExplainKindExt::MetaData;
    }
    else
        return false;

    auto explain_query = std::make_shared<ASTExplainQueryExt>(kind);

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
    ASTPtr query;
    if (kind == ASTExplainQueryExt::ExplainKindExt::ParsedAST)
    {
        ParserQuery p(end, allow_settings_after_format_in_insert);
        if (p.parse(pos, query, expected))
            explain_query->setExplainedQuery(std::move(query));
        else
            return false;
    }
    else if (select_p.parse(pos, query, expected) || create_p.parse(pos, query, expected) || insert_p.parse(pos, query, expected))
        explain_query->setExplainedQuery(std::move(query));
    else
        return false;

    node = std::move(explain_query);
    return true;
}

}
