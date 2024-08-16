#include <Poco/NumberFormatter.h>
#include <Common/DNSResolver.h>
#include <Common/checkStackSize.h>
#include <Common/isLocalAddress.h>
#include <Common/setThreadName.h>
#include <Common/CurrentMetrics.h>
#include <Storages/Consensus/WriteBufferFromFiFoBuffer.h>
#include <Storages/Consensus/RequestQueue.h>
#include <Storages/Consensus/RaftDispatcher.h>


namespace CurrentMetrics
{
    extern const Metric RaftThreads;
    extern const Metric RaftThreadsActive;
    extern const Metric RaftThreadsScheduled;
}

namespace DB
{

using namespace Consensus;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
    extern const int SYSTEM_ERROR;
    extern const int RAFT_ERROR;
}

namespace fs = std::filesystem;
using Poco::NumberFormatter;

RaftDispatcher::RaftDispatcher()
    :log(&Poco::Logger::get("RaftDispatcher"))
{
}

RaftDispatcher::~RaftDispatcher()
{
    shutdown();
}

void RaftDispatcher::initialize(const Poco::Util::AbstractConfiguration & config)
{
    LOG_DEBUG(log, "Initializing dispatcher");
    settings = Consensus::Settings::loadFromConfig(config);

    max_wait = settings->raft_settings->operation_timeout_ms;
    size_t request_thread_count = settings->request_thread_count;

    requests_queue = std::make_shared<RequestQueue>(1, settings->request_queue_size);
    responses_queue = std::make_shared<RaftResponseQueue>(std::numeric_limits<size_t>::max());

    server = std::make_shared<RaftServer>(settings, config, *responses_queue);

    try
    {
        LOG_DEBUG(log, "Waiting server to initialize");
        server->startup();
        LOG_DEBUG(log, "Server initialized, waiting for quorum");
        server->waitInit();
        LOG_DEBUG(log, "Quorum initialized");
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }

    request_forwarder = std::make_shared<RequestForwarder>(responses_queue);
    request_forwarder->initialize(settings->forward_thread_count, max_wait,
        settings->raft_settings->election_timeout_lower_bound_ms, server);

    request_thread = std::make_shared<ThreadPool>(CurrentMetrics::RaftThreads,
        CurrentMetrics::RaftThreadsActive, CurrentMetrics::RaftThreadsScheduled,
        request_thread_count);

    for (size_t i = 0; i < request_thread_count; i++)
    {
        request_thread->trySchedule([this] { requestThread(); });
    }
    response_thread = std::make_unique<ThreadFromGlobalPool>([this] { responseThread(); });
    update_configuration_thread = std::make_unique<ThreadFromGlobalPool>([this] { updateConfigurationThread(); });
    updateConfiguration(config);
    LOG_DEBUG(log, "Dispatcher initialized");
}

void RaftDispatcher::shutdown()
{
    try
    {
        {
            std::lock_guard lock(push_request_mutex);

            if (shutdown_called)
                return;

            LOG_INFO(log, "Shutting down dispatcher");
            shutdown_called = true;

            LOG_INFO(log, "Shutting down update_configuration_thread");
            update_configuration_queue.finish();
            if (update_configuration_thread && update_configuration_thread->joinable())
                update_configuration_thread->join();

            if (requests_queue)
            {
                requests_queue->finish();
            }

            LOG_INFO(log, "Shutting down request_thread");
            if (request_thread)
                request_thread->wait();

            responses_queue->finish();
            LOG_INFO(log, "Shutting down response_thread");
            if (response_thread && response_thread->joinable())
                response_thread->join();
        }

        if (request_forwarder)
            request_forwarder->shutdown();

        if (server)
            server->shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    LOG_INFO(log, "Shut down dispatcher");
}

void RaftDispatcher::requestThread()
{
    setThreadName("RaftReq");
    while (!shutdown_called)
    {
        RaftRequestPtr request;

        if (requests_queue->tryPopAny(request, std::max(max_wait, wait_time_ms)))
        {
            if (shutdown_called)
                break;
            try
            {
                {
                    std::lock_guard lock(process_response_mutex);
                    finish_callbacks[request->id] = request->finish_callback;
                }

                auto send_request = [this] (RaftRequestPtr & req)
                {
                    if (server->isLeader())
                    {
                        LOG_DEBUG(log, "Leader server put request {}, retry count {}", toString(req->id), req->retry_count);
                        server->putRequest(req);
                    }
                    else
                    {
                        LOG_DEBUG(log, "Follower server forward request {}, retry count {}", toString(req->id), req->retry_count);
                        request_forwarder->putRequest(req);
                    }
                    req->retry_count ++;
                };
                request->retry_callback = send_request;
                send_request(request);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }
}

void RaftDispatcher::responseThread()
{
    setThreadName("RaftRes");
    RaftResponsePtr response;
    while (!shutdown_called)
    {
        if (responses_queue->tryPop(response, std::min(max_wait, wait_time_ms)))
        {
            if (shutdown_called)
                break;
            try
            {
                std::map<UUID, RaftFinishCallback>::iterator it;
                {
                    std::lock_guard lock(process_response_mutex);
                    it = finish_callbacks.find(response->id);
                }

                if (it != finish_callbacks.end())
                {
                    //Invoke callback
                    it->second(response);
                    {
                        std::lock_guard lock(process_response_mutex);
                        finish_callbacks.erase(it);
                    }
                }
                else
                    LOG_WARNING(log,"Can't find reponse callback, id {}", toString(response->id));
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }
}

void RaftDispatcher::updateConfigurationThread()
{
    setThreadName("UpdateConfig");
    while (true)
    {
        if (shutdown_called)
            return;
        try
        {
            if (!server->isInit())
            {
                LOG_INFO(log, "Server still not initialized, will not apply configuration until initialization finished");
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                continue;
            }

            ConfigUpdateAction action;
            if (!update_configuration_queue.tryPop(action, 1000))
                continue;

            /// We must wait this update from leader or apply it ourself (if we are leader)
            bool done = false;
            while (!done)
            {
                if (shutdown_called)
                    return;

                if (isLeader())
                {
                    done = server->applyConfigurationUpdate(action);
                    if (!done)
                        LOG_WARNING(log, "Cannot apply configuration update, maybe trying to remove leader node (ourself), will retry");
                }
                else
                {
                    done = server->waitConfigurationUpdate(action);
                    if (!done)
                        LOG_WARNING(
                            log,
                            "Cannot wait for configuration update, maybe we become leader, or maybe update is invalid, will try to wait "
                            "one more time");
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

bool RaftDispatcher::putRequest(const RaftRequestPtr & request)
{
    using namespace std::chrono;
    request->create_time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

    LOG_TRACE(log, "[putRequest]Opnum {}", Consensus::toString(request->getOpNum()));

    if (!requests_queue->tryPush(std::move(request), wait_time_ms))
        throw Exception(
            ErrorCodes::TIMEOUT_EXCEEDED,
            "Cannot push request to queue within operation timeout, requests_queue size {}",
            requests_queue->size());
    return true;
}

void RaftDispatcher::updateConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    auto diff = server->getConfigurationDiff(config);
    if (diff.empty())
        LOG_TRACE(log, "Configuration update triggered, but nothing changed for RAFT");
    else if (diff.size() > 1)
        LOG_WARNING(log, "Configuration changed for more than one server ({}) from cluster, it's strictly not recommended", diff.size());
    else
        LOG_DEBUG(log, "Configuration change size ({})", diff.size());

    for (auto & change : diff)
    {
        bool push_result = update_configuration_queue.push(change);
        if (!push_result)
            throw Exception(ErrorCodes::SYSTEM_ERROR, "Cannot push configuration update to queue");
    }
}

bool RaftDispatcher::isServerActive() const
{
    return isInit() && hasLeader();
}

static uint64_t getDirSize(const fs::path & dir)
{
    checkStackSize();
    if (!fs::exists(dir))
        return 0;

    fs::directory_iterator it(dir);
    fs::directory_iterator end;

    uint64_t size{0};
    while (it != end)
    {
        if (it->is_regular_file())
            size += fs::file_size(*it);
        else
            size += getDirSize(it->path());
        ++it;
    }
    return size;
}

uint64_t RaftDispatcher::getLogDirSize() const
{
    return getDirSize(settings->log_dir);
}

}
