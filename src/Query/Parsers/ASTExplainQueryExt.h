#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSelectWithUnionQuery.h>


namespace DB
{


/// AST, EXPLAIN or other query with meaning of explanation query instead of execution
class ASTExplainQueryExt : public ASTQueryWithOutput
{
public:
    enum ExplainKindExt
    {
        ParsedAST, /// 'EXPLAIN AST SELECT ...'
        AnalyzedSyntax, /// 'EXPLAIN SYNTAX SELECT ...'
        QueryPlan, /// 'EXPLAIN SELECT ...'
        QueryPipeline, /// 'EXPLAIN PIPELINE ...'
        MaterializedView, /// 'EXPLAIN VIEW SELECT ...'
        QueryElement, /// 'EXPLAIN ELEMENT ...'
        PlanSegment, /// 'EXPLAIN PLANSEGMENT ...'
        OptimizerPlan, /// 'EXPLAIN OPT_PLAN ...'
        PreWhereEffect, /// 'EXPLAIN PREWHERE_EFFECT ...'
        DistributedAnalyze, /// 'EXPLAIN ANALYZE DISTRIBUTED SELECT...'
        Distributed, /// 'EXPLAIN DISTRIBUTED SELECT...'
        LogicalAnalyze, /// 'EXPLAIN ANALYZE SELECT...'
        PipelineAnalyze, /// 'EXPLAIN ANALYZE PIPELINE SELECT...'
        TraceOptimizer, /// 'EXPLAIN TRACE_OPT SELECT...'
        TraceOptimizerRule, /// 'EXPLAIN TRACE_OPT RULE SELECT...'
        MetaData, // 'EXPLAIN METADATA...'
    };

    explicit ASTExplainQueryExt(ExplainKindExt kind_) : kind(kind_) { }

    String getID(char delim) const override { return "Explain" + (delim + toString(kind)); }
    ExplainKindExt getKind() const { return kind; }
    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTExplainQueryExt>(*this);

        res->children.clear();
        if (query)
            res->setExplainedQuery(query);
        if (ast_settings)
            res->setSettings(ast_settings);
        cloneOutputOptions(*res);
        return res;
    }

    // ASTType getType() const override { return ASTType::ASTExplainQuery; }

    void setExplainedQuery(ASTPtr query_)
    {
        children.emplace_back(query_);
        query = std::move(query_);
    }

    void setSettings(ASTPtr settings_)
    {
        children.emplace_back(settings_);
        ast_settings = std::move(settings_);
    }

    const ASTPtr & getExplainedQuery() const
    {
        auto * select_query = query->as<ASTSelectWithUnionQuery>();
        if (select_query && !select_query->out_file && out_file)
            cloneOutputOptions(*select_query);
        return query;
    }
    ASTPtr & getExplainedQuery() { return query; }
    const ASTPtr & getSettings() const { return ast_settings; }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << toString(kind) << (settings.hilite ? hilite_none : "");

        if (ast_settings)
        {
            settings.ostr << ' ';
            ast_settings->formatImpl(settings, state, frame);
        }

        settings.ostr << settings.nl_or_ws;
        query->formatImpl(settings, state, frame);
    }

private:
    ExplainKindExt kind;

    ASTPtr query;
    ASTPtr ast_settings;

    static String toString(ExplainKindExt kind)
    {
        switch (kind)
        {
            case ParsedAST:
                return "EXPLAIN AST";
            case AnalyzedSyntax:
                return "EXPLAIN SYNTAX";
            case QueryPlan:
                return "EXPLAIN";
            case QueryPipeline:
                return "EXPLAIN PIPELINE";
            case MaterializedView:
                return "EXPLAIN VIEW";
            case QueryElement:
                return "EXPLAIN ELEMENT";
            case PlanSegment:
                return "EXPLAIN PLANSEGMENT";
            case OptimizerPlan:
                return "EXPLAIN OPT_PLAN";
            case PreWhereEffect:
                return "EXPLAIN PREWHERE_EFFECT";
            case DistributedAnalyze:
                return "EXPLAIN ANALYZE DISTRIBUTED";
            case LogicalAnalyze:
                return "EXPLAIN ANALYZE";
            case PipelineAnalyze:
                return "EXPLAIN ANALYZE PIPELINE";
            case Distributed:
                return "EXPLAIN DISTRIBUTED";
            case TraceOptimizer:
                return "EXPLAIN TRACE_OPT";
            case TraceOptimizerRule:
                return "EXPLAIN TRACE_OPT RULE";
            case MetaData:
                return "EXPLAIN METADATA";
        }

        UNREACHABLE();
    }
};

}
