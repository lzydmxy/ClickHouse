#include "StreamHandler.h"
#include <brpc/stream.h>
#include <base/types.h>
#include <Common/logger_useful.h>
#include <IO/ReadHelpers.h>
#include <Processors/Chunk.h>
#include <Compression/CompressedReadBuffer.h>
#include <Query/Common/MultiPathBoundedQueue.h>
#include <Query/Exchange/DataTrans/DeserializeBufTransform.h>
#include <Query/Exchange/bRPC/ReadBufferFromBrpc.h>
#include <Query/Exchange/DataTrans/NativeChunkInputStream.h>

namespace DB
{

int StreamHandler::on_received_messages([[maybe_unused]] brpc::StreamId stream_id, butil::IOBuf * const messages[], size_t size) noexcept
{
    BrpcRemoteBroadcastReceiverShardPtr receiver_ptr = receiver.lock();
    if (!receiver_ptr)
    {
        LOG_WARNING(log, "on_received_messages receiver is expired, stream {}", stream_id);
        return 0;
    }
    try
    {
        if (!keep_order)
        {
            for (size_t index = 0; index < size; index++)
            {
                butil::IOBuf * msg = messages[index];
#ifndef NDEBUG
                LOG_TRACE(log, "on_received_messages stream {} received exchange data size {}",
                    stream_id, msg->size());
#endif
                Chunk chunk;
                if (optimizer_context->getSettingsRef().log_query_exchange)
                {
                    auto chunk_info = std::make_shared<DeserializeBufTransform::IOBufChunkInfoWithReceiver>();
                    chunk_info->io_buf.append(msg->movable());
                    chunk_info->receiver = receiver_ptr;
                    chunk.setChunkInfo(std::move(chunk_info));
                }
                else
                {
                    auto chunk_info = std::make_shared<DeserializeBufTransform::IOBufChunkInfo>();
                    chunk_info->io_buf.append(msg->movable());
                    chunk.setChunkInfo(std::move(chunk_info));
                }
                receiver_ptr->pushReceiveQueue(DataPacket{std::move(chunk)});
            }
            return 0;
        }
        for (size_t index = 0; index < size; index++)
        {
            Stopwatch s;
            butil::IOBuf & msg = *messages[index];
            auto read_buffer = std::make_unique<ReadBufferFromBrpc>(msg);
            std::unique_ptr<ReadBuffer> buf;
            if (optimizer_context->getSettingsRef().exchange_enable_block_compress)
                buf = std::make_unique<CompressedReadBuffer>(*read_buffer);
            else
                buf = std::move(read_buffer);
            NativeChunkInputStream chunk_in(*buf, header);
            Chunk chunk = chunk_in.readImpl();
            if (optimizer_context->getSettingsRef().log_query_exchange)
            {
                auto chunk_info = std::make_shared<DeserializeBufTransform::IOBufChunkInfoWithReceiver>();
                chunk_info->receiver = receiver_ptr;
            }
            receiver_ptr->addToMetricsMaybe(0, s.elapsedMilliseconds(), 0, msg);
#ifndef NDEBUG
            LOG_TRACE(log, "on_received_messages, stream {} received exchange data size {}, chunk rows {}",
                stream_id, msg.size(), chunk.getNumRows());
#endif
            receiver_ptr->pushReceiveQueue(MultiPathDataPacket(DataPacket{std::move(chunk)}));
        }
    }
    catch (...)
    {
        try
        {
            String exception_str = getCurrentExceptionMessage(true);
            auto current_status = receiver_ptr->finish(BroadcastStatusCode::RECV_TIMEOUT, exception_str);
            if (current_status.is_modified_by_operator)
                LOG_WARNING(log, "on_received_messages pushReceiveQueue exception: {}", exception_str);
        }
        catch (...)
        {
            LOG_WARNING(log, "on_received_messages finish receiver exception: {}", getCurrentExceptionMessage(true));
        }
    }
    return 0;
}

void StreamHandler::on_idle_timeout(brpc::StreamId id)
{
    try
    {
        LOG_WARNING(log, "on_idle_timeout stream {}", id);
    }
    catch (...)
    {
        LOG_WARNING(log, "on_idle_timeout stream {} exception: {}", id, getCurrentExceptionMessage(true));
    }
}

void StreamHandler::on_closed(brpc::StreamId stream_id)
{
    try
    {
        BrpcRemoteBroadcastReceiverShardPtr receiver_ptr = receiver.lock();
        if (!receiver_ptr)
        {
            LOG_WARNING(log, "on_closed receiver is expired, stream {}", stream_id);
        }
        else
        {
            LOG_DEBUG(log, "on_closed stream {}, receiver {}", stream_id, receiver_ptr->getName());
            auto status = receiver_ptr->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "Try close receiver gracefully");
            if (status.is_modified_by_operator && status.code == BroadcastStatusCode::ALL_SENDERS_DONE)
            {
                // Push an empty as finish to close receiver gracefully
                receiver_ptr->pushReceiveQueue(MultiPathDataPacket(receiver_ptr->getName()));
            }
        }
    }
    catch (...)
    {
        LOG_WARNING(log, "on_closed exception: {}", getCurrentExceptionMessage(true));
    }
}

void StreamHandler::on_failed(brpc::StreamId id, int32_t error_code, const std::string& error_text)
{
    chassert(error_code > 0);
    try
    {
        BrpcRemoteBroadcastReceiverShardPtr receiver_ptr = receiver.lock();
        if (!receiver_ptr)
        {
            LOG_WARNING(log, "on_failed stream {}, receiver is expired.", id);
        }
        else
        {
            ///Only care about finish status which need close receiver immediately
            LOG_INFO(log, "on_failed stream {}, receiver {}, code {}", id, receiver_ptr->getName(), error_code);
            if (error_code > 0)
                receiver_ptr->finish(static_cast<BroadcastStatusCode>(error_code), "StreamHandler::on_failed called:" + error_text);
            else
                receiver_ptr->setSendDoneFlag();
        }
    }
    catch (...)
    {
        LOG_WARNING(log, "on_failed stream {}, exception: {}", id, getCurrentExceptionMessage(true));
    }
}

}
