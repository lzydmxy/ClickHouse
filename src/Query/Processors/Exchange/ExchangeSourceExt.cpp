#include <Query/Processors/Exchange/ExchangeSourceExt.h>

#include <QueryPipeline/RemoteQueryExecutor.h>
#include <QueryPipeline/RemoteQueryExecutorReadContext.h>
#include <Query/Executor/SegmentScheduler.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Common/Exception.h>

#include <algorithm>
#include <atomic>
#include <optional>
#include <variant>

namespace DB
{
namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED_INTERNAL;
    extern const int EXCHANGE_DATA_TRANS_EXCEPTION;
    extern const int TIMEOUT_EXCEEDED;
}

class ExchangeTotalsSource;
using ExchangeTotalsSourcePtr = std::shared_ptr<ExchangeTotalsSourceExt>;
class ExchangeExtremesSource;
using ExchangeExtremesSourcePtr = std::shared_ptr<ExchangeExtremesSourceExt>;

ExchangeSourceExt::ExchangeSourceExt(
    Block header_,
    BroadcastReceiverPtr receiver_,
    ExchangeOptions options_,
    ExchangeTotalsSourcePtr totals_source_,
    ExchangeExtremesSourcePtr extremes_source_)
    : ISource(std::move(header_), false)
    , receiver(std::move(receiver_))
    , options(options_)
    , totals_source(std::move(totals_source_))
    , extremes_source(std::move(extremes_source_))
    , logger(getLogger("ExchangeSourceExt"))
{
}

ExchangeSourceExt::ExchangeSourceExt(
    Block header_,
    BroadcastReceiverPtr receiver_,
    ExchangeOptions options_,
    bool,
    ExchangeTotalsSourcePtr totals_source_,
    ExchangeExtremesSourcePtr extremes_source_)
    : ISource(std::move(header_), false)
    , receiver(std::move(receiver_))
    , options(options_)
    , totals_source(std::move(totals_source_))
    , extremes_source(std::move(extremes_source_))
    , logger(getLogger("ExchangeSourceExt"))
{
}

ExchangeSourceExt::~ExchangeSourceExt() = default;

/// ClassName[Property](Incude class)
String ExchangeSourceExt::getName() const
{
    return fmt::format("ExchangeSourceExt({})", receiver->getName());
}

String ExchangeSourceExt::getClassName() const
{
    return "ExchangeSourceExt";
}

IProcessor::Status ExchangeSourceExt::prepare()
{
    LOG_TRACE(logger, "{} begin prepare", getName());
    const auto & status = ISource::prepare();
    LOG_TRACE(logger, "{} parent's prepare, status is {}", getName(), ISource::statusToName(status));
    if (status == Status::Finished)
    {
        receiver->finish(BroadcastStatusCode::RECV_REACH_LIMIT, "ExchangeSourceExt finished");
    }
    return status;
}

std::optional<Chunk> ExchangeSourceExt::tryGenerate()
{
    if (was_query_canceled || was_receiver_finished)
        return std::nullopt;

    RecvDataPacket packet = receiver->recv(options.exchange_timeout_ts);

    if (std::holds_alternative<Chunk>(packet))
    {
        Chunk chunk = std::move(std::get<Chunk>(packet));
#ifndef NDEBUG
        LOG_TRACE(logger, "{} receive chunk with rows {}", getName(), chunk.getNumRows());
#endif
        if (chunk && chunk.getChunkInfo() &&  getChunkType(chunk.getChunkInfo()) == ChunkType::Totals && totals_source)
        {
            totals_source->setTotals(std::move(chunk)); // assuming only one totals chunk, so it should be safe to do so.
            chunk = {};
        }
        else if (chunk && chunk.getChunkInfo() &&  getChunkType(chunk.getChunkInfo()) == ChunkType::Extremes && extremes_source)
        {
            extremes_source->setExtremes(std::move(chunk)); // assuming only one extremes chunk, so it should be safe to do so.
            chunk = {};
        }
        return std::make_optional(std::move(chunk));
    }
    else
    {
        const auto & status = std::get<BroadcastStatus>(packet);
#ifndef NDEBUG
        LOG_TRACE(logger, "{} recv status is {}", getName(), status.code);
#endif
        checkBroadcastStatus(status);
        was_receiver_finished = true;
        return std::nullopt;
    }
}

void ExchangeSourceExt::work()
{
    try
    {
        read_progress_was_set = false;

        if (auto chunk = tryGenerate())
        {
            current_chunk.chunk = std::move(*chunk);
            if (current_chunk.chunk || current_chunk.chunk.getChunkInfo())
            {
                has_input = true;
                if (auto_progress && !read_progress_was_set)
                    progress(current_chunk.chunk.getNumRows(), current_chunk.chunk.bytes());// TODO wujianchao chunk is always empty
            }
        }
        else
            finished = true;

        if (isCancelled())
            finished = true;
    }
    catch (...)
    {
        finished = true;
        got_exception = true;
        throw;
    }
}

void ExchangeSourceExt::onCancel()
{
    LOG_TRACE(logger, "{} onCancel", getName());
    was_query_canceled = true;
    receiver->finish(BroadcastStatusCode::RECV_CANCELLED, "Cancelled by pipeline");
}

void ExchangeSourceExt::checkBroadcastStatus(const BroadcastStatus & status) const
{
    // Better fix me. Using `>` is not a good practice to determine next move, as the `BroadcastStatusCode` may be added casually.
    if (status.code > BroadcastStatusCode::RECV_REACH_LIMIT)
    {
        if (status.is_modified_by_operator)
        {
            if(status.code == BroadcastStatusCode::RECV_TIMEOUT)
            {
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Query {} receive data timeout, maybe you can increase settings max_execution_time. Debug info for source {}: {}",
                    CurrentThread::getQueryId(), getName(), status.message);
            }
            else
            {
                // CANCELLED, NOT_READY, UNKNOWN_ERROR
                throw Exception(ErrorCodes::EXCHANGE_DATA_TRANS_EXCEPTION,
                    "Query {} cancels receiving data due to unknown reason with code {} and error message {}. The real error message may "
                    "be in log or query_log. Exchange source name is {}",
                    CurrentThread::getQueryId(), status.code, status.message, getName());
            }
        }

        // If receiver is finished and not cancelly by pipeline, we should cancel pipeline here
        if (status.code != BroadcastStatusCode::RECV_CANCELLED)
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED_INTERNAL,
                "Query {} cancels receiving data due to unknown reason with code {} and error message {}. The real error message may "
                "be in log or query_log. Exchange source name is {}",
                CurrentThread::getQueryId(), status.code, status.message, getName());
    }
}

ExchangeTotalsSourceExt::ExchangeTotalsSourceExt(const Block& header)
    : ISource(header)
{
}

ExchangeTotalsSourceExt::~ExchangeTotalsSourceExt() = default;

Chunk ExchangeTotalsSourceExt::generate()
{
    return std::move(totals);
}

void ExchangeTotalsSourceExt::setTotals(Chunk chunk)
{
    totals = std::move(chunk);
}

ExchangeExtremesSourceExt::ExchangeExtremesSourceExt(const Block& header)
    : ISource(header)
{
}

ExchangeExtremesSourceExt::~ExchangeExtremesSourceExt() = default;

Chunk ExchangeExtremesSourceExt::generate()
{
    return std::move(extremes);
}

void ExchangeExtremesSourceExt::setExtremes(Chunk chunk)
{
    extremes = std::move(chunk);
}

}
