#include <base/sleep.h>
#include <Common/setThreadName.h>
#include <Common/CurrentMetrics.h>
#include <Storages/Consensus/RaftDispatcher.h>
#include <Storages/Consensus/RequestForwarder.h>


namespace CurrentMetrics
{
    extern const Metric RaftThreads;
    extern const Metric RaftThreadsActive;
    extern const Metric RaftThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int SYSTEM_ERROR;
    extern const int RAFT_ERROR;
    extern const int NETWORK_ERROR;
}

void RequestForwarder::putRequest(RaftRequestPtr & request)
{
    LOG_DEBUG(log, "Put request {}", request->toString());
    requests_queue->push(std::move(request));
}

void RequestForwarder::runForward(RunnerID thread_idx)
{
    setThreadName(("ReqFwdSend-" + toString(thread_idx)).c_str());

    LOG_DEBUG(log, "Starting forwarding request sending thread.");
    while (!shutdown_called)
    {
        UInt64 max_wait = pop_timeout_ms;
        ServerRequest request;
        request.protocol = PkgType::Forward;
        if (requests_queue->tryPop(thread_idx, request.raft_request, max_wait))
        {
            auto raft_response = request.raft_request->makeResponse();
            try
            {
                if (!server->isLeader() && server->isLeaderAlive())
                {
                    request.leader_id = server->getLeader();
                    request.thread_id = thread_idx;
                    auto client = server->getLeaderClient(thread_idx);
                    if (client)
                    {
                        LOG_DEBUG(log, "Send forward request to server, leader id {}, thread id {}",
                            request.leader_id, request.thread_id);

                        client->send(request);

                        ServerResponse response;
                        client->receive(response);
                        if (!response.accepted)
                        {
                            LOG_ERROR(log, "Receive failed forward server response with type {}",
                                toString(response.protocol));
                        }
                        raft_response = response.raft_response;
                    }
                    else
                    {
                        raft_response->error = Error::ZCONNECTIONLOSS;
                        throw Exception(ErrorCodes::NETWORK_ERROR, "Not found client for leader {}, thread {}",
                            server->getLeader(), std::to_string(thread_idx));
                    }
                }
                else
                {
                    raft_response->error = Error::ZRUNTIMEINCONSISTENCY;
                    throw Exception(ErrorCodes::RAFT_ERROR, "Raft no leader");
                }
            }
            catch (...)
            {
                if (raft_response->error == Error::ZOK)
                    raft_response->error = Error::NURAFTERROR;
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }

            if ((raft_response->error == Error::ZCONNECTIONLOSS
                || raft_response->error == Error::ZRUNTIMEINCONSISTENCY
                || raft_response->error == Error::NURAFTERROR)
                && request.raft_request->needRetry())
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(retry_timeout_ms));
                LOG_DEBUG(log, "Retry invoke request, retry count {}, retry sleep milli second {}",
                    request.raft_request->retry_count, retry_timeout_ms);
                request.raft_request->retry_callback(request.raft_request);
            }
            else
            {
                if (!responses_queue->push(raft_response))
                {
                    throw Exception(ErrorCodes::SYSTEM_ERROR,
                        "Could not push error response id {} to responses queue",
                        raft_response->id);
                }
            }
        }
    }
}

void RequestForwarder::shutdown()
{
    LOG_INFO(log, "Shutting down request forwarder");
    if (shutdown_called)
        return;
    shutdown_called = true;
    if (request_thread)
        request_thread->wait();
    LOG_INFO(log, "Shut down request forwarder");
}

void RequestForwarder::initialize(
    size_t forward_thread_count_,
    int32_t retry_timeout_ms_,
    int32_t pop_timeout_ms_,
    RaftServerPtr server_)
{
    forward_thread_count = forward_thread_count_;
    retry_timeout_ms = retry_timeout_ms_;
    pop_timeout_ms = pop_timeout_ms_;
    server = server_;

    requests_queue = std::make_shared<RequestQueue>(forward_thread_count, 8);
    request_thread = std::make_shared<ThreadPool>(CurrentMetrics::RaftThreads,
        CurrentMetrics::RaftThreadsActive, CurrentMetrics::RaftThreadsScheduled,
        forward_thread_count);
    for (RunnerID thread_idx = 0; thread_idx < forward_thread_count; thread_idx++)
    {
        request_thread->trySchedule([this, thread_idx] { runForward(thread_idx); });
    }
}

}
