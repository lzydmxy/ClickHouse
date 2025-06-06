#pragma once

#include <Interpreters/IInterpreter.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Query/Executor/ProfileElementConsumer.h>
#include <Query/Parsers/ASTExplainQueryExt.h>
#include <Query/Processors/QueryPlan/QueryPlanExt.h>
#include <Common/Logger.h>

namespace DB
{

/// Returns single row with explain results
class InterpreterExplainQueryUseOptimizer : public IInterpreter, WithMutableContext
{
public:
    InterpreterExplainQueryUseOptimizer(const ASTPtr & query_, ContextMutablePtr context_)
        : WithMutableContext(context_), query(query_), log(getLogger("InterpreterExplainQueryUseOptimizer"))
    {
    }

    BlockIO execute() override;

    Block getSampleBlock();

    static void fillColumn(IColumn & column, const std::string & str);

private:
    ASTPtr query;
    LoggerPtr log;
    SelectQueryOptions options;

    QueryPipeline executeImpl();

    void rewriteDistributedToLocal(ASTPtr & ast);

    void elementDatabaseAndTable(const ASTSelectQuery & select_query, const ASTPtr & where, WriteBuffer & buffer);

    void elementWhere(const ASTPtr & where, WriteBuffer & buffer);

    void elementDimensions(const ASTPtr & select, WriteBuffer & buffer);

    void elementMetrics(const ASTPtr & select, WriteBuffer & buffer);

    void elementGroupBy(const ASTPtr & group_by, WriteBuffer & buffer);

    void listPartitionKeys(StoragePtr & storage, WriteBuffer & buffer);

    void listRowsOfOnePartition(StoragePtr & storage, const ASTPtr & group_by, const ASTPtr & where, WriteBuffer & buffer);

    std::optional<String> getActivePartCondition(StoragePtr & storage);

    QueryPipeline explain();

    QueryPipeline explainUsingOptimizer();

    QueryPipeline explainMetaData();

    void explainPlanWithOptimizer(
        const ASTExplainQueryExt & explain_ast,
        QueryPlanExt & plan,
        WriteBuffer & buffer,
        ContextMutablePtr & context_ptr,
        bool & single_line);

    void explainDistributedWithOptimizer(
        const ASTExplainQueryExt & explain_ast, QueryPlanExt & plan, WriteBuffer & buffer, ContextMutablePtr & context_ptr);

    BlockIO explainAnalyze();

    void explainPipelineWithOptimizer(
        const ASTExplainQueryExt & explain_ast, QueryPlanExt & plan, WriteBuffer & buffer, ContextMutablePtr & context_ptr);
};


class ExplainConsumer : public ProfileElementConsumer<ProcessorProfileLogElement>
{
public:
    explicit ExplainConsumer(std::string query_id) : ProfileElementConsumer(query_id) { }
    void consume(ProcessorProfileLogElement & element) override;

    std::vector<ProcessorProfileLogElement> getStoreResult() const { return store_vector; }
    std::vector<ProcessorProfileLogElement> store_vector;
};

}
