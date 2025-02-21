#pragma once

#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSelectWithUnionQuery.h>


namespace DB
{

/// AST, EXPLAIN or other query with meaning of explanation query instead of execution
class ASTExplainQueryExt : public ASTExplainQuery
{
public:
    enum ExplainKindExt
    {
        ParsedAST, /// 'EXPLAIN AST SELECT ...'
        AnalyzedSyntax, /// 'EXPLAIN SYNTAX SELECT ...'
        QueryPlan, /// 'EXPLAIN SELECT ...'
        QueryTree, /// 'EXPLAIN QUERY TREE SELECT ...'
        QueryPipeline, /// 'EXPLAIN PIPELINE ...'
        QueryEstimates, /// 'EXPLAIN ESTIMATE ...'
        TableOverride, /// 'EXPLAIN TABLE OVERRIDE ...'
        CurrentTransaction, /// 'EXPLAIN CURRENT TRANSACTION'
        MaterializedView, /// 'EXPLAIN VIEW SELECT ...'
        QueryElement, /// 'EXPLAIN ELEMENT ...'
        PlanSegment, /// 'EXPLAIN PLANSEGMENT ...'
        OptimizerPlan, /// 'EXPLAIN OPT_PLAN ...'
        PreWhereEffect, /// 'EXPLAIN PREWHERE_EFFECT ...'
        DistributedAnalyze, /// 'EXPLAIN ANALYZE DISTRIBUTED SELECT...'
        Distributed, /// 'EXPLAIN DISTRIBUTED SELECT...'
        LogicalAnalyze,    /// 'EXPLAIN ANALYZE SELECT...'
        PipelineAnalyze,    /// 'EXPLAIN ANALYZE PIPELINE SELECT...'
        TraceOptimizer,    /// 'EXPLAIN TRACE_OPT SELECT...'
        TraceOptimizerRule,    /// 'EXPLAIN TRACE_OPT RULE SELECT...'
        MetaData, // 'EXPLAIN METADATA...'
    };

    ASTExplainQueryExt(ExplainKind kind_) : ASTExplainQuery(kind_) { }
    explicit ASTExplainQueryExt(ExplainKindExt kind_, ExplainKind base_kind_) : ASTExplainQuery(base_kind_), kind(kind_) {}

    String getID(char delim) const override { return "ExplainExt" + (delim + toString(kind)); }
    ExplainKindExt getKind() const { return kind; }
    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTExplainQueryExt>(*this);

        res->children.clear();
        auto query = ASTExplainQuery::clone();
        if (query)
            res->setExplainedQuery(query);
        auto ast_settings = getSettings();
        if (ast_settings)
            res->setSettings(ast_settings);
        cloneOutputOptions(*res);
        return res;
    }

private:
    ExplainKindExt kind;

    static String toString(ExplainKindExt kind)
    {
        switch (kind)
        {
            case ParsedAST: return "EXPLAIN AST";
            case AnalyzedSyntax: return "EXPLAIN SYNTAX";
            case QueryPlan: return "EXPLAIN";
            case QueryTree: return "EXPLAIN QUERY TREE";
            case QueryPipeline: return "EXPLAIN PIPELINE";
            case QueryEstimates: return "EXPLAIN ESTIMATE";
            case TableOverride: return "EXPLAIN TABLE OVERRIDE";
            case CurrentTransaction: return "EXPLAIN CURRENT TRANSACTION";
            case MaterializedView: return "EXPLAIN VIEW";
            case QueryElement: return "EXPLAIN ELEMENT";
            case PlanSegment: return "EXPLAIN PLANSEGMENT";
            case OptimizerPlan: return "EXPLAIN OPT_PLAN";
            case PreWhereEffect: return "EXPLAIN PREWHERE_EFFECT";
            case DistributedAnalyze: return "EXPLAIN ANALYZE DISTRIBUTED";
            case LogicalAnalyze: return "EXPLAIN ANALYZE";
            case PipelineAnalyze: return "EXPLAIN ANALYZE PIPELINE";
            case Distributed: return "EXPLAIN DISTRIBUTED";
            case TraceOptimizer: return "EXPLAIN TRACE_OPT";
            case TraceOptimizerRule: return "EXPLAIN TRACE_OPT RULE";
            case MetaData: return "EXPLAIN METADATA";
        }

        __builtin_unreachable();
    }
};

}
