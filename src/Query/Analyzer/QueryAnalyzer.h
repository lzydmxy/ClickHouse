#pragma once

#include <Interpreters/Context_fwd.h>
#include <Query/Analyzer/Analysis.h>


namespace DB
{

    class QueryAnalyzer
    {
    public:
        /**
         * Entry method of analyzing phase.
         *
         */
        static AnalysisPtr analyze(ASTPtr & ast, ContextPtr context);

        /**
         * Entry method of analyzing a cross-scoped ASTSelectWithUnionQuery/ASTSelectIntersectExceptQuery/ASTSelectQuery(e.g. subquery expression, correlated join).
         *
         */
        static void analyze(ASTPtr & query, ScopePtr outer_query_scope, ContextPtr context, Analysis & analysis);
    };

}
