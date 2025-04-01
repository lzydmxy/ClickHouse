#include "BroadcastExchangeSink.h"
#include <Processors/ISource.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Query/Exchange/DataTrans/IBroadcastSender.h>

namespace DB
{
BroadcastExchangeSink::BroadcastExchangeSink(Block header_, BroadcastSenderPtrs senders_, ExchangeOptions options_, const String &name_)
    : IExchangeSink(std::move(header_))
    , name(name_)
    , senders(std::move(senders_))
    , options(std::move(options_))
    , buffer_chunk(getPort().getHeader(), options.send_threshold_in_bytes, options.send_threshold_in_row_num)
    , logger(getLogger("BroadcastExchangeSink"))
{
    if (options.force_use_buffer)
        buffer_chunk.resetBuffer();
}

BroadcastExchangeSink::~BroadcastExchangeSink() = default;


void BroadcastExchangeSink::consume(Chunk chunk)
{
    LOG_TRACE(logger, "BroadcastExchangeSink consume");
    if (options.force_use_buffer)
    {
        auto chunk_to_send = buffer_chunk.flush(true);
        if (chunk_to_send)
        {
            for (auto & sender : senders)
                ExchangeUtils::sendAndCheckReturnStatus(*sender, chunk_to_send.clone());
        }
    }

    Chunk chunk_to_send;
    if (options.force_use_buffer)
    {
        chunk_to_send = buffer_chunk.add(std::move(chunk));
        if (!chunk_to_send)
            return;
    }
    else
    {
        chunk_to_send = std::move(chunk);
    }

    bool has_active_sender = false;
    for (size_t i = 0; i < senders.size() - 1; ++i)
    {
        auto status = ExchangeUtils::sendAndCheckReturnStatus(*senders[i], chunk_to_send.clone());
        if (status.code == BroadcastStatusCode::RUNNING)
            has_active_sender = true;
    }

    auto status = ExchangeUtils::sendAndCheckReturnStatus(*senders.back(), std::move(chunk_to_send));
    if (status.code == BroadcastStatusCode::RUNNING)
        has_active_sender = true;

    if (!has_active_sender)
        onFinish();
}

void BroadcastExchangeSink::onFinish()
{
    LOG_TRACE(logger, "BroadcastExchangeSink onFinish");
    IExchangeSink::onFinish();
}

void BroadcastExchangeSink::onCancel()
{
    LOG_TRACE(logger, "BroadcastExchangeSink onCancel");
    for (auto & sender : senders)
    {
        sender->finish(BroadcastStatusCode::SEND_CANCELLED, "Cancelled by pipeline");
    }
}

}
