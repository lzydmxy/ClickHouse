#include <filesystem>
#include <Poco/File.h>
#include <base/sleep.h>
#include <IO/VarInt.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Common/setThreadName.h>
#include <Storages/Consensus/RaftStateManager.h>
#include <Storages/Consensus/RaftLogStorage.h>


namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
}

}

namespace Consensus
{

RaftStateManager::RaftStateManager(
    int server_id_,
    const Poco::Util::AbstractConfiguration & config_,
    SettingsPtr settings_)
    : settings(settings_), svr_id(server_id_), my_host(settings_->host), my_internal_port(settings_->internal_port)
    , committed_index(0), committed_queue(settings_->request_queue_size)
    , log(&(Poco::Logger::get("RaftStateManager")))
{
    curr_log_store = RaftNew<RaftLogStorage>(settings->log_dir, settings);
    auto state_dir = settings->state_dir;
    if (!Poco::File(state_dir).exists())
    {
        Poco::File(state_dir).createDirectory();
        LOG_DEBUG(log, "Create state directory {}", state_dir);
    }

    cluster_config_file = fs::path(state_dir) / "cluster_config";
    srv_state_file = fs::path(state_dir) / "srv_state";
    log_state_file = fs::path(state_dir) / "log_state";
    snapshot_state_file = fs::path(state_dir) / "snapshot_state";
    cur_cluster_config = parseClusterConfig(config_, "raft_server.cluster", settings->request_thread_count);

    loadCommittedIndex();

    save_index_thread = std::make_unique<ThreadFromGlobalPool>([this] { saveIndexThread(); });
}

NuClusterConfigPtr RaftStateManager::load_config()
{
    if (!Poco::File(cluster_config_file).exists())
    {
        LOG_INFO(log, "Load config with initial cluster config.");
        return cur_cluster_config;
    }

    std::unique_ptr<ReadBufferFromFile> read_file_buf = std::make_unique<ReadBufferFromFile>(cluster_config_file, 4096);
    size_t size;
    readVarUInt(size, *read_file_buf);
    NuBufferPtr buf = NuBuffer::alloc(size);
    read_file_buf->readStrict(reinterpret_cast<char *>(buf->data()), size);
    cur_cluster_config = NuClusterConfig::deserialize(*buf);
    LOG_INFO(log, "Load config with log index {}, prev log index {}",
        cur_cluster_config->get_log_idx(), cur_cluster_config->get_prev_log_idx());
    return cur_cluster_config;
}

void RaftStateManager::save_config(const cluster_config & config)
{
    std::unique_ptr<WriteBufferFromFile> out_file_buf
        = std::make_unique<WriteBufferFromFile>(cluster_config_file, 4096, O_WRONLY | O_TRUNC | O_CREAT);
    NuBufferPtr data = config.serialize();
    writeVarUInt(data->size(), *out_file_buf);
    out_file_buf->write(reinterpret_cast<char *>(data->data()), data->size());
    out_file_buf->finalize();
    out_file_buf->sync();
    LOG_INFO(log, "Save config with log index {}, prev log index {}",
        config.get_log_idx(), config.get_prev_log_idx());
    cur_cluster_config = cluster_config::deserialize(*data);
}

void RaftStateManager::save_state(const srv_state & state)
{
    std::ofstream out(srv_state_file, std::ios::binary | std::ios::trunc);

    auto data = state.serialize();
    out.write(reinterpret_cast<char *>(data->data()), data->size());
    out.close();

    LOG_INFO(log, "Save srv_state with term {} and vote_for {}", state.get_term(), state.get_voted_for());
}

NuServerStatePtr RaftStateManager::read_state()
{
    std::ifstream in(srv_state_file, std::ios::binary);

    if (!in)
    {
        LOG_WARNING(log, "Raft srv_state file not exist, maybe this is the first startup.");
        return RaftNew<NuServerState>();
    }
    in.seekg(0, std::ios::end);
    size_t size = in.tellg();

    in.seekg(0, std::ios::beg);
    char data[size];

    in.read(data, size);
    in.close();

    NuBufferPtr buf = NuBuffer::alloc(size);
    buf->put_raw(reinterpret_cast<const nuraft::byte *>(data), size);

    auto ret = srv_state::deserialize(*buf.get());
    LOG_INFO(log, "Load srv_state: term {}, vote_for {} from disk", ret->get_term(), ret->get_voted_for());

    return ret;
}

void RaftStateManager::saveCommittedIndex(UInt64 committed_index_)
{
    if (!committed_queue.tryPush(committed_index_, settings->raft_settings->operation_timeout_ms))
        throw Exception(
            ErrorCodes::TIMEOUT_EXCEEDED,
            "Cannot push committed to queue within operation timeout, committed_queue size {}",
            committed_queue.size());
    committed_index.store(committed_index_);
}

void RaftStateManager::saveIndexThread()
{
    LOG_INFO(log, "Create log state {}", log_state_file);
    std::unique_ptr<WriteBufferFromFile> out_file;
    while(!shutdown_called)
    {
        UInt64 save_index;
        if (committed_queue.tryPop(save_index, settings->raft_settings->operation_timeout_ms))
        {
            if (out_file == nullptr)
                out_file = std::make_unique<WriteBufferFromFile>(log_state_file, 128, O_WRONLY | O_TRUNC | O_CREAT);
            out_file->seek(0, SEEK_SET);
            writeVarUInt(save_index, *out_file);
            out_file->sync();
            LOG_TRACE(log, "Saved committed index {}, last committed index {}", save_index, committed_index.load());
        }
    }
    out_file->close();
    LOG_INFO(log, "Exit save thread");
}

void RaftStateManager::loadCommittedIndex()
{
    LOG_INFO(log, "Load log state file {}", log_state_file);
    if (Poco::File(log_state_file).exists())
    {
        auto read_file = std::make_unique<ReadBufferFromFile>(log_state_file, 128);
        UInt64 val;
        readVarUInt(val, *read_file);
        read_file->close();
        committed_index.store(val);
        LOG_INFO(log, "Load log committed index {}", val);
    }
    else
        LOG_INFO(log, "File {} is not exists", log_state_file);
}

NuSnapshotPtr RaftStateManager::mergeSnapshotState(NuSnapshot & snap_st, UInt64 & committed_index_)
{
    return std::make_shared<NuSnapshot>(static_cast<ulong>(committed_index_),
        snap_st.get_last_log_term(), snap_st.get_last_config(),
        snap_st.size(), snap_st.get_type());
}

void RaftStateManager::saveSnapshotState(NuSnapshot & snapshot)
{
    WriteBufferFromFile out_file(snapshot_state_file, 4096, O_WRONLY | O_TRUNC | O_CREAT);
    auto buf = snapshot.serialize();
    writeVarUInt(buf->size(), out_file);
    out_file.write(reinterpret_cast<char *>(buf->data()), buf->size());
    out_file.close();

    LOG_DEBUG(log, "Save snapshot, term {} index {}",
        snapshot.get_last_log_term(), snapshot.get_last_log_idx());
}

NuSnapshotPtr RaftStateManager::loadSnapshotState()
{
    if (Poco::File(snapshot_state_file).exists())
    {
        ReadBufferFromFile read_file(snapshot_state_file, 4096);
        size_t size;
        readVarUInt(size, read_file);
        auto buf = NuBuffer::alloc(size);
        read_file.readStrict(reinterpret_cast<char *>(buf->data()), size);
        auto snapshot_state = NuSnapshot::deserialize(*buf);

        LOG_INFO(log, "Load snapshot state with log term {}, log index {}",
            snapshot_state->get_last_log_term(), snapshot_state->get_last_log_idx());
        return snapshot_state;
    }
    else
    {
        LOG_INFO(log, "File {} is not exists", snapshot_state_file);
        return nullptr;
    }
}

void RaftStateManager::system_exit(const int exit_code)
{
    shutdown_called = true;
    committed_queue.finish();
    if (save_index_thread)
        save_index_thread->join();
    LOG_INFO(log, "Raft system exit with code {}", exit_code);
}

NuClusterConfigPtr RaftStateManager::parseClusterConfig(
    const Poco::Util::AbstractConfiguration & config, const std::string & config_name, size_t forward_thread_count) const
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_name, keys);

    auto ret_cluster_config = RaftNew<NuClusterConfig>();
    {
        std::lock_guard<std::mutex> lock(clients_mutex);
        clients.clear();

        for (const auto & key : keys)
        {
            if (startsWith(key, "server"))
            {
                int id = config.getInt(config_name + "." + key + ".id");
                String host = config.getString(config_name + "." + key + ".host");
                String internal_port = config.getString(config_name + "." + key + ".internal_port");
                String internal_endpoint = host + ":" + internal_port;
                bool learner = config.getBool(config_name + "." + key + ".learner", false);
                int priority = config.getInt(config_name + "." + key + ".priority", 1);
                ret_cluster_config->get_servers().push_back(RaftNew<NuServerConfig>(id, 0, internal_endpoint, "", learner, priority));

                if (svr_id != id)
                {
                    String port = config.getString(config_name + "." + key + ".port");
                    String endpoint = host + ":" + port;

                    LOG_INFO(log, "Create RaftConnection for current id {}, server id {}, end point {}, thread count {}",
                        id, svr_id, endpoint, forward_thread_count);

                    for (size_t i = 0; i < forward_thread_count; ++i)
                    {
                        auto & client_list = clients[id];
                        auto client = std::make_shared<RaftConnection>(
                            svr_id, i, endpoint, settings->raft_settings->operation_timeout_ms * 1000);
                        client_list.push_back(client);
                    }
                }
            }
        }

        /// If user does not configure cluster, put myself to NuRaft.
        if (ret_cluster_config->get_servers().empty())
        {
            auto my_endpoint = my_host + ":" + std::to_string(my_internal_port);
            ret_cluster_config->get_servers().push_back(RaftNew<NuServerConfig>(svr_id, 0, my_endpoint, "", false, 1));
        }
    }

    std::string s;
    std::for_each(ret_cluster_config->get_servers().cbegin(), ret_cluster_config->get_servers().cend(), [&s](NuServerConfigPtr srv) {
        s += " ";
        s += srv->get_endpoint();
    });

    LOG_INFO(log, "Raft cluster config : {}", s);
    return ret_cluster_config;
}

ConfigUpdateActions RaftStateManager::getConfigurationDiff(const Poco::Util::AbstractConfiguration & config) const
{
    auto new_cluster_config = parseClusterConfig(config, "raft_server.cluster", settings->forward_thread_count);

    std::unordered_map<int, NuServerConfigPtr> new_ids, old_ids;
    for (const auto & new_server : new_cluster_config->get_servers())
        new_ids[new_server->get_id()] = new_server;

    {
        for (const auto & old_server : cur_cluster_config->get_servers())
            old_ids[old_server->get_id()] = old_server;
    }

    ConfigUpdateActions result;

    /// First that remove old ones
    for (auto [old_id, server_config] : old_ids)
    {
        if (!new_ids.count(old_id))
            result.emplace_back(ConfigUpdateAction{ConfigUpdateActionType::RemoveServer, server_config});
    }

    /// After of all add new servers
    for (auto [new_id, server_config] : new_ids)
    {
        if (!old_ids.count(new_id))
            result.emplace_back(ConfigUpdateAction{ConfigUpdateActionType::AddServer, server_config});
    }

    {
        /// And update priority if required
        for (const auto & old_server : cur_cluster_config->get_servers())
        {
            for (const auto & new_server : new_cluster_config->get_servers())
            {
                if (old_server->get_id() == new_server->get_id())
                {
                    if (old_server->get_priority() != new_server->get_priority())
                    {
                        result.emplace_back(ConfigUpdateAction{ConfigUpdateActionType::UpdatePriority, new_server});
                    }
                    break;
                }
            }
        }
    }

    return result;
}

RaftConnectionPtr RaftStateManager::getClient(int32_t leader_id, RunnerID thread_idx)
{
    std::lock_guard<std::mutex> lock(clients_mutex);
    if (clients.contains(leader_id) && clients[leader_id].size() > thread_idx)
    {
        return clients[leader_id][thread_idx];
    }
    return nullptr;
}

}
