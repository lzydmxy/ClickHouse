#include "BrpcProxy.h"
#include <brpc/socket.h>
#include <Common/logger_useful.h>
#include <Query/Common/QueryCommon.h>

namespace DB
{

const static int32_t STREAM_RUNNING = 0;

BrpcProxy BrpcProxy::brpc_proxy;

bool BrpcProxy::setStreamStatus(StreamId stream_id, int32_t status_code)
{
    std::unique_lock<std::shared_mutex> lock(stream_mutex);
    stream_status[stream_id] = status_code;
    return true;
}

void BrpcProxy::clearStreamStatus(StreamId stream_id)
{
    std::unique_lock<std::shared_mutex> lock(stream_mutex);
    auto it = stream_status.find(stream_id);
    if (it != stream_status.end())
        stream_status.erase(it);
    else
        LOG_WARNING(log, "Cant find stream {} status", stream_id);
}

int32_t BrpcProxy::getStreamStatus(StreamId stream_id)
{
    std::shared_lock<std::shared_mutex> lock(stream_mutex);
    auto it = stream_status.find(stream_id);
    if (it != stream_status.end())
    {
        return it->second;
    }
    else
    {
        LOG_WARNING(log, "Cant find stream {} status", stream_id);
        return 0;
    }
}

int BrpcProxy::StreamCreate(StreamId* request_stream, Controller &cntl, const StreamOptions* options)
{
    setStreamStatus(*request_stream, STREAM_RUNNING);
    return brpc::StreamCreate(request_stream, cntl, options);
}

int BrpcProxy::StreamAccept(StreamId* response_stream, Controller &cntl, const StreamOptions* options)
{
    return brpc::StreamAccept(response_stream, cntl, options);
}

int BrpcProxy::StreamWrite(StreamId stream_id, const butil::IOBuf &message)
{
    return brpc::StreamWrite(stream_id, message);
}

int BrpcProxy::StreamWait(StreamId stream_id, const TimePoint & due_time)
{
    auto due_span = chronoToTimespec(due_time);
    return brpc::StreamWait(stream_id, &due_span);
}

void BrpcProxy::StreamWait(StreamId stream_id, const TimePoint & due_time
        , void (*on_writable)(StreamId stream_id, void* arg, int error_code)
        , void *arg)
{
    auto due_span = chronoToTimespec(due_time);
    brpc::StreamWait(stream_id, &due_span, on_writable, arg);
}

int BrpcProxy::StreamClose(StreamId stream_id)
{
    clearStreamStatus(stream_id);
    return brpc::StreamClose(stream_id);
}

int BrpcProxy::StreamFinish(StreamId stream_id, int32_t &actual_fin_code, int32_t expected_fin_code, bool finish_remote_stream)
{
    SocketUniquePtr ptr;
    if (Socket::AddressFailedAsWell(stream_id, &ptr) == -1)
    {
        return EINVAL;
    }
    if (!setStreamStatus(stream_id, expected_fin_code)) 
    {
        actual_fin_code = getStreamStatus(stream_id);
        return -1;
    }
    if (finish_remote_stream && !ptr->Failed()) {
        // TODO: Write expected_fin_code to stream
        // Stream *s = (Stream *) ptr->conn();
        // s->SendStreamFin(expected_fin_code);

    }
    actual_fin_code = expected_fin_code;

    return 0;
}

int BrpcProxy::StreamFinishedCode(StreamId stream_id, int32_t& fin_status_code)
{
    fin_status_code = getStreamStatus(stream_id);
    return 0;
}

}
