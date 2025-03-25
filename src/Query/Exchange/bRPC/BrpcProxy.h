#pragma once
#include <brpc/stream.h>
#include <brpc/controller.h>
#include <Query/Common/QueryCommon.h>

namespace DB
{

using namespace brpc;

/**
 * Provide external interface to shield the modification differences between BRCP and BC
 */
class BrpcProxy
{
public:
    static BrpcProxy & getInstance()
    {
        static BrpcProxy brpc_proxy;
        return brpc_proxy;
    }

    // [Called at the client side]
    // Create a stream at client-side along with the |cntl|, which will be connected
    // when receiving the response with a stream from server-side. If |options| is
    // NULL, the stream will be created with default options
    // Return 0 on success, -1 otherwise
    int StreamCreate(StreamId* request_stream, Controller &cntl, const StreamOptions* options);

    // [Called at the server side]
    // Accept the stream. If client didn't create a stream with the request 
    // (cntl.has_remote_stream() returns false), this method would fail.
    // Return 0 on success, -1 otherwise.
    int StreamAccept(StreamId* response_stream, Controller &cntl, const StreamOptions* options);

    // Write |message| into |stream_id|. The remote-side handler will received the 
    // message by the written order
    // Returns 0 on success, errno otherwise
    // Errno:
    //  - EAGAIN: |stream_id| is created with positive |max_buf_size| and buf size
    //            which the remote side hasn't consumed yet excceeds the number.
    //  - EINVAL: |stream_id| is invalied or has been closed
    int StreamWrite(StreamId stream_id, const butil::IOBuf &message);

    // Write util the pending buffer size is less than |max_buf_size| or orrur
    // occurs
    // Returns 0 on success, errno otherwise
    // Errno:
    //  - ETIMEDOUT: when |due_time| is not NULL and time expired this
    //  - EINVAL: the stream was close during waiting
    int StreamWait(StreamId stream_id, const TimePoint & due_time);

    // Async wait
    void StreamWait(StreamId stream_id, const TimePoint & due_time
        , void (*on_writable)(StreamId stream_id, void* arg, int error_code)
        , void *arg);

    // Close |stream_id|, after this function is called:
    //  - All the following |StreamWrite| would fail 
    //  - |StreamWait| wakes up immediately.
    //  - Both sides |on_closed| would be notifed after all the pending buffers have
    //    been received
    // This function could be called multiple times without side-effects
    int StreamClose(StreamId stream_id);

    int StreamFinish(StreamId stream_id, int32_t &actual_fin_code, int32_t expected_fin_code, bool finish_remote_stream);

    int StreamFinishedCode(StreamId stream_id, int32_t& fin_status_code);

private:
    BrpcProxy()
    {
    }
    bool setStreamStatus(StreamId stream_id, int32_t status_code);
    void clearStreamStatus(StreamId stream_id);
    int32_t getStreamStatus(StreamId stream_id);
    LoggerPtr log = getLogger("BrpcProxy");
    std::shared_mutex stream_mutex;
    std::map<StreamId, int32_t> stream_status;
};

}
