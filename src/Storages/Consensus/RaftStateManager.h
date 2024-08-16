#pragma once

#include <fstream>
#include <Poco/Util/LayeredConfiguration.h>
#include <Common/logger_useful.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/StringUtils/StringUtils.h>
#include <Storages/Consensus/NuRaft.h>
#include <Storages/Consensus/Settings.h>
#include <Storages/Consensus/RaftConnection.h>



namespace Consensus
{

/// When our configuration changes the following action types
/// can happen
enum class ConfigUpdateActionType
{
    RemoveServer,
    AddServer,
    UpdatePriority,
};

/// Action to update configuration
struct ConfigUpdateAction
{
    ConfigUpdateActionType action_type;
    NuServerConfigPtr server;
};

using ConfigUpdateActions = std::vector<ConfigUpdateAction>;

class RaftStateManager : public nuraft::state_mgr
{
public:
    RaftStateManager(
        int server_id_,
        const Poco::Util::AbstractConfiguration & config_,
        SettingsPtr settings_);

    ~RaftStateManager() override = default;

    NuClusterConfigPtr parseClusterConfig(const Poco::Util::AbstractConfiguration & config, const String & config_name, size_t thread_count) const;

    NuClusterConfigPtr load_config() override;

    void save_config(const NuClusterConfig & config) override;

    void save_state(const NuServerState & state) override;

    NuServerStatePtr read_state() override;

    NuLogStorePtr load_log_store() override { return curr_log_store; }

    int32 server_id() override { return svr_id; }

    void system_exit(const int exit_code) override;

    bool shouldStartAsFollower() const { return start_as_follower_servers.count(svr_id); }

    //ptr<srv_config> get_srv_config() const { return curr_srv_config; }

    NuClusterConfigPtr get_cluster_config() const { return cur_cluster_config; }

    /// Get configuration diff between proposed XML and current state in RAFT
    ConfigUpdateActions getConfigurationDiff(const Poco::Util::AbstractConfiguration & config) const;

    RaftConnectionPtr getClient(int32_t leader_id, RunnerID thread_idx);

    /// Async save committed index to file
    void saveCommittedIndex(UInt64 committed_index);
    UInt64 getCommittedIndex() { return committed_index.load(); }
    void saveIndexThread();

    void saveSnapshotState(NuSnapshot & snapshot);
    NuSnapshotPtr loadSnapshotState();

    //merge log state to snapshot state, keep the snapshot's committed index is newest.
    NuSnapshotPtr mergeSnapshotState(NuSnapshot & snap_st, UInt64 & committed_index_);
protected:
    RaftStateManager() = delete;

private:
    void loadCommittedIndex();

private:
    SettingsPtr settings;
    int svr_id;

    String my_host;
    int32_t my_internal_port;
    std::atomic<UInt64> committed_index;
    ConcurrentBoundedQueue<UInt64> committed_queue;

    std::unordered_set<int> start_as_follower_servers;
    NuLogStorePtr curr_log_store;
    NuClusterConfigPtr cur_cluster_config;

    GlobalThreadPtr save_index_thread;
    bool shutdown_called { false };

    mutable std::mutex clients_mutex;
    mutable std::unordered_map<UInt32, std::vector<RaftConnectionPtr>> clients;

    Poco::Logger * log;
    std::string srv_state_file;
    std::string cluster_config_file;
    std::string log_state_file;
    std::string snapshot_state_file;
};

using RaftStateManagerPtr = std::shared_ptr<RaftStateManager>;

}
