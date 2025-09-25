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
        LOG_WARNING(log, "on_received_messages. StreamId-{}'s receiver is expired", stream_id);
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
                LOG_TRACE(log, "on_received_messages. StreamId-{} received exchange data successfully, io-buffer size: {}",
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
                chunk.setColumns(header.getColumns(), 0);
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
            LOG_TRACE(log, "on_received_messages. StreamId-{} received exchange data successfully, io-buffer size: {}, chunk rows: {}",
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
                LOG_ERROR(log, "on_received_messages. StreamId-{} pushReceiveQueue exception happen-{}", stream_id, exception_str);
        }
        catch (...)
        {
            LOG_WARNING(log, "on_received_messages. StreamId-{} finish receiver exception happen-{}", stream_id, getCurrentExceptionMessage(true));
        }
    }
    return 0;
}

void StreamHandler::on_idle_timeout(brpc::StreamId stream_id)
{
    try
    {
        LOG_WARNING(log, "on_idle_timeout. StreamId-{} idle timeout.", stream_id);
    }
    catch (...)
    {
        LOG_WARNING(log, "on_idle_timeout. StreamId-{} exception happen-{}", stream_id, getCurrentExceptionMessage(true));
    }
}

void StreamHandler::on_closed(brpc::StreamId stream_id)
{
    try
    {
        BrpcRemoteBroadcastReceiverShardPtr receiver_ptr = receiver.lock();
        if (!receiver_ptr)
        {
            LOG_WARNING(log, "on_closed. StreamId-{}'s receiver is expired.", stream_id);
        }
        else
        {
            LOG_DEBUG(log, "on_closed. StreamId-{} closing, datakey: {} ", stream_id, receiver_ptr->getName());
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
        LOG_WARNING(log, "on_closed. StreamId-{} close receiver exception happen-{}", stream_id, getCurrentExceptionMessage(true));
    }
}

void StreamHandler::on_failed(brpc::StreamId stream_id, int32_t error_code, const std::string& error_text)
{
    try
    {
        BrpcRemoteBroadcastReceiverShardPtr receiver_ptr = receiver.lock();
        if (!receiver_ptr)
        {
            LOG_WARNING(log, "on_finished. StreamId-{}'s receiver is expired.", stream_id);
        }
        else
        {
            LOG_INFO(log, "on_finished. StreamId-{} finishing, datakey: {}, error_code: {}, error_text: {}", stream_id, receiver_ptr->getName(), error_code, error_text);
            receiver_ptr->finish(static_cast<BroadcastStatusCode>(RECV_UNKNOWN_ERROR), "StreamHandler::on_failed called");
        }
    }
    catch (...)
    {
        LOG_WARNING(log, "on_finished. StreamId-{} finish receiver exception happen-{}", stream_id, getCurrentExceptionMessage(true));
    }
}

void StreamHandler::on_finished(brpc::StreamId stream_id, int32_t finish_status_code)
{
    try
    {
        BrpcRemoteBroadcastReceiverShardPtr receiver_ptr = receiver.lock();
        if (!receiver_ptr)
        {
            LOG_WARNING(log, "on_finished. StreamId-{}'s receiver is expired.", stream_id);
        }
        else
        {
            ///Only care about finish status which need close receiver immediately
            LOG_INFO(log, "on_finished. StreamId-{} finishing, datakey: {}, finish_status_code: {}", stream_id, receiver_ptr->getName(), finish_status_code);
            if (finish_status_code > 0)
                receiver_ptr->finish(static_cast<BroadcastStatusCode>(finish_status_code), "StreamHandler::on_finished called");
            else
                receiver_ptr->setSendDoneFlag();
        }
    }
    catch (...)
    {
        LOG_WARNING(log, "on_finished. StreamId-{} finish receiver exception happen-{}", stream_id, getCurrentExceptionMessage(true));
    }
}

}
