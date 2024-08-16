#pragma once

#include <memory>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ThreadPool.h>

#include <Storages/Consensus/Settings.h>
#include <Storages/Consensus/NuRaft.h>
#include <Storages/Consensus/RaftCommon.h>
#include <Storages/Consensus/RaftServer.h>
#include <Storages/Consensus/RaftStateMachine.h>
#include <Storages/Consensus/RaftConnectionStats.h>
#include <Storages/Consensus/RequestForwarder.h>
#include <Storages/Consensus/RequestQueue.h>


namespace DB
{

class RaftDispatcher : public std::enable_shared_from_this<RaftDispatcher>
{
public:
    RaftDispatcher();
    ~RaftDispatcher();

    void initialize(const Poco::Util::AbstractConfiguration & config);

    void shutdown();

    bool putRequest(const RaftRequestPtr & request);

    /// Registered in ConfigReloader callback. Add new configuration changes to
    /// update_configuration_queue. Keeper Dispatcher apply them asynchronously.
    void updateConfiguration(const Poco::Util::AbstractConfiguration & config);

    /// Invoked when a request completes.
    void updateStatLatency(uint64_t process_time_ms);

    bool isServerActive() const;
    bool isInit() const { return server && server->isInit(); }
    /// Are we leader
    bool isLeader() const { return server->isLeader(); }
    bool hasLeader() const { return server->isLeaderAlive(); }
    bool isObserver() const { return server->isObserver(); }
    bool requestLeader() { return server->requestLeader(); }
    RaftLogInfo getLogInfo() { return server->getLogInfo(); }
    const SettingsPtr getSettings() const { return settings; }
    const RaftStateMachinePtr getStateMachine() const { return server->getStateMachine(); }

    uint64_t getLogDirSize() const;

    Int32 max_wait_ms() { return max_wait; }

private:
    Poco::Logger * log;

    std::atomic<bool> shutdown_called{false};
    Consensus::SettingsPtr settings;

    RaftServerPtr server;
    std::mutex push_request_mutex;
    RequestQueuePtr requests_queue;

    std::mutex process_response_mutex;
    RaftResponseQueuePtr responses_queue;
    std::map<UUID, RaftFinishCallback> finish_callbacks;

    ThreadPoolPtr request_thread;
    GlobalThreadPtr response_thread;

    Int32 wait_time_ms  = 1000L;
    Int32 max_wait;

    RequestForwarderPtr request_forwarder;
    std::mutex forward_response_mutex;

    using UpdateConfigurationQueue = ConcurrentBoundedQueue<ConfigUpdateAction>;
    /// More than 1k updates is definitely misconfiguration.
    UpdateConfigurationQueue update_configuration_queue{1000};
    /// Apply or wait for configuration changes
    GlobalThreadPtr update_configuration_thread;

    void requestThread();
    void responseThread();
    /// Thread apply or wait configuration changes from leader
    void updateConfigurationThread();
};

using RaftDispatcherPtr = std::shared_ptr<RaftDispatcher>;

}
