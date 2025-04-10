#pragma once

#include <Interpreters/Context_fwd.h>
#include <Query/Interpreters/InterpreterSelectQueryUseOptimizer.h>
#include <Query/Common/Utils.h>

namespace DB
{

    ContextMutablePtr createContextForSubQuery(ContextPtr context, String sub_query_tag = "");
    ContextMutablePtr getContextWithNewTransaction(const ContextPtr & context, bool read_only, bool with_auth = false);

    void executeSubQueryWithoutResult(const String & query, ContextMutablePtr query_context, bool internal = false);

    Block executeSubQueryWithOneRow(
        const String & query, ContextMutablePtr query_context, bool internal = false, bool tolerate_multi_rows = true);

    void executeSubQuery(const String & query, ContextMutablePtr query_context, std::function<void(Block &)> proc_block, bool internal = false);

    Block executeSubPipelineWithOneRow(
        const ASTPtr & query,
        ContextMutablePtr query_context,
        std::function<void(InterpreterSelectQueryUseOptimizer &)> pre_execute,
        bool tolerate_multi_rows = true);

    void executeSubPipeline(
        const ASTPtr & query,
        ContextMutablePtr query_context,
        std::function<void(InterpreterSelectQueryUseOptimizer &)> pre_execute,
        std::function<void(Block &)> proc_block);

    bool needThrowRootCauseError(const Context * context, int & error_code, String & error_messge);
}
