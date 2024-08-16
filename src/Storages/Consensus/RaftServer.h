#pragma once

#include <unordered_map>
#include <Storages/Consensus/RaftCommon.h>
#include <Storages/Consensus/RaftStorage.h>
#include <Storages/Consensus/RaftStateMachine.h>
#include <Storages/Consensus/RaftStateManager.h>
#include <Storages/Consensus/RaftLogStorage.h>
#include <Storages/Consensus/Settings.h>


namespace DB
{

struct ServerInfo
{
    UInt32 server_id;
    std::string endpoint;
    bool is_leader;
};

/// Raft log information for 4lw commands
struct RaftLogInfo
{
    uint64_t first_log_idx;     /// My first log index in log store.
    uint64_t first_log_term;    /// My first log term.
    uint64_t last_log_idx;      /// My last log index in log store.
    uint64_t last_log_term;     /// My last log term.
    uint64_t last_committed_log_idx;    /// My last committed log index in state machine.
    uint64_t leader_committed_log_idx;  /// Leader's committed log index from my perspective.
    uint64_t target_committed_log_idx;  /// Target log index should be committed to.
    uint64_t last_snapshot_idx; /// The largest committed log index in last snapshot.
};

class RaftServer
{
public:
    RaftServer(
        const SettingsPtr & settings_,
        const Poco::Util::AbstractConfiguration & config_,
        RaftResponseQueue & responses_queue_);

    void startup();

    void waitInit();

    /// Return true if RaftServer initialized
    bool isInit() const { return initialized_flag; }

    void shutdown();

    void putRequest(const RaftRequestPtr & request);

    CMDResultPtr putRequestBatch(const RaftRequests & request_batch);

    RaftStateMachinePtr getStateMachine() const { return state_machine; }
    RaftStateManagerPtr getStateManager() const { return state_manager; }

    RaftConnectionPtr getLeaderClient(RunnerID thread_idx);

    int32 getLeader();

    bool isLeader() const;

    bool isLeaderAlive() const;

    bool isFollower() const;

    bool isObserver() const;

    /// @return follower count if node is not leader return 0
    uint64_t getFollowerCount() const;

    /// @return synced follower count if node is not leader return 0
    uint64_t getSyncedFollowerCount() const;

    /// Get configuration diff between current configuration in RAFT and in XML file
    ConfigUpdateActions getConfigurationDiff(const Poco::Util::AbstractConfiguration & config_);

    /// Apply action for configuration update. Actually call raft_instance->remove_srv or raft_instance->add_srv.
    /// Synchronously check for update results with retries.
    bool applyConfigurationUpdate(const ConfigUpdateAction & task);

    /// Wait configuration update for action. Used by followers.
    /// Return true if update was successfully received.
    bool waitConfigurationUpdate(const ConfigUpdateAction & task);

    /// Callback func which is called by NuRaft on all internal events.
    /// Used to determine the moment when raft is ready to server new requests
    nuraft::cb_func::ReturnCode callbackFunc(nuraft::cb_func::Type type, nuraft::cb_func::Param * param);

    /// Raft log information
    RaftLogInfo getLogInfo();

    bool requestLeader();
private:
    std::atomic<bool> initialized_flag = false;
    std::mutex initialized_mutex;
    std::mutex append_entries_mutex;
    std::condition_variable initialized_cv;

    int server_id;
    SettingsPtr settings;
    const Poco::Util::AbstractConfiguration & config;
    RaftResponseQueue & responses_queue;
    Poco::Logger * log;

    NuRaftLauncher launcher;
    NuRaftServerPtr raft_instance;
    RaftStateMachinePtr state_machine;
    RaftStateManagerPtr state_manager;
};

using RaftServerPtr = std::shared_ptr<RaftServer>;

}
