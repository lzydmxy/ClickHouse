#include <Query/Interpreters/executeSubQuery.h>

#include <Core/UUID.h>
#include <IO/NullWriteBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Interpreters/Context.h>
#include <Query/Executor/QueryMPPManager.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_ROWS;
}

ContextMutablePtr createContextForSubQuery(ContextPtr context, String sub_query_tag)
{
    auto query_context = Context::createCopy(context);
    query_context->makeSessionContext();
    query_context->makeQueryContext();

    auto parent_initial_query_id = context->getInitialQueryId();
    auto uuid = UUIDHelpers::UUIDToString(UUIDHelpers::generateV4());
    String initial_query_id;

    if (sub_query_tag.empty())
        initial_query_id = fmt::format("{}_{}", context->getCurrentQueryId(), uuid);
    else
        initial_query_id = fmt::format("{}_{}_{}", context->getCurrentQueryId(), sub_query_tag, uuid);

    // todo: zhangwanyun1, need parent_initial_query_id from ClientInfo
    // query_context->getClientInfo().parent_initial_query_id = parent_initial_query_id;
    query_context->getClientInfo().initial_query_id = initial_query_id;
    query_context->setCurrentQueryId(initial_query_id);

    return query_context;
}

bool needThrowRootCauseError(const Context * context, int & error_code, String & error_messge)
{
    const String & query_id = context->getCurrentQueryId();
    auto coordinator = QueryMPPManager::instance().getCoordinator(query_id);
    if (!coordinator)
        return false;

    if (coordinator->getContext().get() != context)
        return false;

    coordinator->updateSegmentInstanceStatus(
        RuntimeSegmentStatus{.query_id = query_id, .segment_id = 0, .is_succeed = false, .metrics=RuntimeSegmentsMetrics(), .message = error_messge, .code = error_code});
    if (isAmbiguosError(error_code))
    {
        auto query_status = coordinator->waitUntilFinish(error_code, error_messge);
        if (query_status.error_code != error_code)
        {
            error_code = query_status.error_code;
            error_messge = query_status.summarized_error_msg;
            return true;
        }
    }
    return false;
}

void modifyQueryContext(ContextMutablePtr query_context, bool internal)
{
    if (internal)
        query_context->setInternalQuery(internal);
}

std::exception_ptr constructException(ContextMutablePtr query_context)
{
    auto exception_code = getCurrentExceptionCode();
    auto exception = getCurrentExceptionMessage(false);

    bool throw_root_cause = needThrowRootCauseError(query_context.get(), exception_code, exception);
    if (!throw_root_cause)
        exception = fmt::format("Query [{}] failed with : {}", query_context->getCurrentQueryId(), exception);

    return std::make_exception_ptr(Exception(exception_code, "Query [{}] failed with : {}", query_context->getCurrentQueryId(), exception));
}

void executeSubQueryWithoutResult(const String & query, ContextMutablePtr query_context, bool internal)
{
    modifyQueryContext(query_context, internal);

    std::exception_ptr exception;
    auto thread = ThreadFromGlobalPool([context = std::move(query_context), &query, internal, &exception]() {
        try
        {
            CurrentThread::QueryScope query_scope{context};
            {
                ReadBufferFromOwnString in(query);
                NullWriteBuffer out;
                executeQuery(in, out, /*allow_into_outfile=*/false, context, /*set_result_details=*/{}, QueryFlags{.internal=internal}, std::nullopt, {});
            }
        }
        catch (...)
        {
            exception = constructException(context);
        }
    });
    thread.join();

    if (exception)
        std::rethrow_exception(exception);
}

Block executeSubQueryWithOneRow(const String & query, ContextMutablePtr query_context, bool internal, bool tolerate_multi_rows)
{
    modifyQueryContext(query_context, internal);

    Block block;
    std::exception_ptr exception;
    auto thread = ThreadFromGlobalPool([context = std::move(query_context), &query, internal, tolerate_multi_rows, &block, &exception]() {
        try
        {
            CurrentThread::QueryScope query_scope{context};
            {
                auto block_io = executeQuery(query, context, QueryFlags{.internal=internal}).second;

                PullingPipelineExecutor executor(block_io.pipeline);

                while (block.rows() == 0 && executor.pull(block));

                if (!tolerate_multi_rows && block.rows() != 1)
                    throw Exception(ErrorCodes::TOO_MANY_ROWS, "Unexcepted block");

                Block tmp_block;
                while (tmp_block.rows() == 0 && executor.pull(tmp_block))
                {
                    if (tmp_block.rows() > 0)
                        throw Exception(ErrorCodes::TOO_MANY_ROWS, "Unexcepted block");
                }
            }
        }
        catch (...)
        {
            exception = constructException(context);
        }
    });
    thread.join();

    if (exception)
        std::rethrow_exception(exception);

    return block;
}

void executeSubQuery(const String & query, ContextMutablePtr query_context, std::function<void(Block &)> proc_block, bool internal)
{
    modifyQueryContext(query_context, internal);

    std::exception_ptr exception;
    auto thread = ThreadFromGlobalPool([context = std::move(query_context), &query, proc_block, internal, &exception]() {
        try
        {
            CurrentThread::QueryScope query_scope{context};
            {
                auto block_io = executeQuery(query, context, QueryFlags{.internal=internal}).second;

                PullingPipelineExecutor executor(block_io.pipeline);
                Block block;

                while (executor.pull(block))
                {
                    if (block.rows() == 0)
                        continue;
                    proc_block(block);
                }
            }
        }
        catch (...)
        {
            exception = constructException(context);
        }
    });
    thread.join();

    if (exception)
        std::rethrow_exception(exception);
}

Block executeSubPipelineWithOneRow(
    const ASTPtr & query, ContextMutablePtr query_context, std::function<void(InterpreterSelectQueryUseOptimizer &)> pre_execute, bool tolerate_multi_rows)
{
    if (!query->as<ASTSelectWithUnionQuery>())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unrecognized query type '{}' when executing subquery {}",
            query->getID(),
            query->formatForErrorMessage());

    Block block;
    std::exception_ptr exception;
    auto thread = ThreadFromGlobalPool([context = std::move(query_context), &query, pre_execute, tolerate_multi_rows, &block, &exception]() {
        try
        {
            CurrentThread::QueryScope query_scope{context};
            {
                SelectQueryOptions query_options;
                InterpreterSelectQueryUseOptimizer interpreter{query, context, query_options};
                auto block_io = interpreter.execute();
                PullingPipelineExecutor executor(block_io.pipeline);

                pre_execute(interpreter);

                while (block.rows() == 0 && executor.pull(block));

                if (!tolerate_multi_rows && block.rows() != 1)
                    throw Exception(ErrorCodes::TOO_MANY_ROWS, "Unexcepted block");

                Block tmp_block;
                while (tmp_block.rows() == 0 && executor.pull(tmp_block))
                {
                    if (tmp_block.rows() > 0)
                        throw Exception(ErrorCodes::TOO_MANY_ROWS, "Unexcepted block");
                }
            }
        }
        catch (...)
        {
            exception = constructException(context);
        }
    });
    thread.join();

    if (exception)
        std::rethrow_exception(exception);

    return block;
}

void executeSubPipeline(
    const ASTPtr & query,
    ContextMutablePtr query_context,
    std::function<void(InterpreterSelectQueryUseOptimizer &)> pre_execute,
    std::function<void(Block &)> proc_block)
{
    if (!query->as<ASTSelectWithUnionQuery>())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unrecognized query type '{}' when executing subquery {}",
            query->getID(),
            query->formatForErrorMessage());

    std::exception_ptr exception;
    auto thread = ThreadFromGlobalPool([context = std::move(query_context), &query, pre_execute, proc_block, &exception]() {
        try
        {
            CurrentThread::QueryScope query_scope{context};
            {
                SelectQueryOptions query_options;
                InterpreterSelectQueryUseOptimizer interpreter{query, context, query_options};
                auto block_io = interpreter.execute();
                PullingPipelineExecutor executor(block_io.pipeline);

                pre_execute(interpreter);

                Block block;
                while (executor.pull(block))
                {
                    if (block.rows() == 0)
                        continue;
                    proc_block(block);
                }
            }
        }
        catch (...)
        {
            exception = constructException(context);
        }
    });
    thread.join();

    if (exception)
        std::rethrow_exception(exception);
}
}
