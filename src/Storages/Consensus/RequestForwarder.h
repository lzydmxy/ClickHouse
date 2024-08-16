#pragma once

#include <Storages/Consensus/RequestQueue.h>
#include <Storages/Consensus/RaftServer.h>


namespace DB
{

class RaftDispatcher;
using RaftDispatcherPtr = std::shared_ptr<RaftDispatcher>;

class RequestForwarder
{
public:
    explicit RequestForwarder(RaftResponseQueuePtr responses_queue_)
        : responses_queue(responses_queue_)
        , log(&Poco::Logger::get("RequestForwarder"))
    {
    }
    void initialize(
        size_t forward_thread_count_,
        int32_t retry_timeout_ms_,
        int32_t pop_timeout_ms_,
        RaftServerPtr server_);
    void putRequest(RaftRequestPtr & request);
    void shutdown();
private:
    void runForward(RunnerID runner_id);
    size_t forward_thread_count;
    int32_t pop_timeout_ms;
    int32_t retry_timeout_ms;
    RequestQueuePtr requests_queue;
    RaftResponseQueuePtr responses_queue;

    Poco::Logger * log;

    ThreadPoolPtr request_thread;
    bool shutdown_called{false};
    RaftServerPtr server;
};

using RequestForwarderPtr = std::shared_ptr<RequestForwarder>;

}
